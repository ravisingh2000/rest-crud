const postgres = require('postgres');
let log4js = require('log4js');

const utils = require('../utils');
let version = require('../package.json').version;

const logLevel = process.env.LOG_LEVEL || 'trace';
const loggerName = process.env.HOSTNAME ? `[${process.env.DATA_STACK_NAMESPACE}] [${process.env.HOSTNAME}] [REST_CRUD PGSQL ${version}]` : `[REST_CRUD PGSQL ${version}]`;
log4js.configure({
    levels: { AUDIT: { value: Number.MAX_VALUE - 1, colour: 'yellow' } },
    appenders: { out: { type: 'stdout', layout: { type: 'basic' } } },
    categories: { default: { appenders: ['out'], level: logLevel.toUpperCase() } }
});
let logger = log4js.getLogger(loggerName);


function resolveNumber(envValue, defaultValue) {
    const n = parseInt(envValue, 10);
    return Number.isNaN(n) ? defaultValue : n;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function errCode(err) {
    return (err && (err.code || err.errno)) || (err && err.message) || 'unknown';
}

const DATASERVICE_TIMEOUT_MS = parseInt(process.env.DATASERVICE_TIMEOUT, 10) || 60000;
const ACQUIRE_TIMEOUT_DEFAULT_MS = Math.max(5000, Math.floor(DATASERVICE_TIMEOUT_MS * 0.9));
const VALIDATE_TIMEOUT_MS = 3000;

function raceAcquire(promise, ms, code, onLate) {
    let timer, timedOut = false;
    promise.then(
        (v) => { if (timedOut && onLate) { try { onLate(v); } catch (_) {} } },
        () => {}
    );
    return Promise.race([
        promise,
        new Promise((_, reject) => { timer = setTimeout(() => { timedOut = true; reject(Object.assign(new Error(`${code} after ${ms}ms`), { code })); }, ms); })
    ]).finally(() => clearTimeout(timer));
}

function jitter(base) {
    return Math.round(base / 2 + Math.random() * base / 2);
}

const RETRYABLE_PG_CODES = new Set([
    '57P01', '57P02', '57P03', '57P05', '08000', '08003', '08006', '25P03', '53300'
]);

const RETRYABLE_CONN_CODES = new Set([
    'CONNECTION_CLOSED', 'CONNECTION_DESTROYED', 'CONNECTION_ENDED', 'CONNECT_TIMEOUT',
    'CONNECTION_CLOSED_BY_PEER', 'ECONNRESET', 'EPIPE', 'ETIMEDOUT', 'ECONNREFUSED', 'EHOSTUNREACH'
]);

function isRetryableConnectionError(err) {
    if (!err) return false;
    const code = err.code || err.errno;
    if (RETRYABLE_CONN_CODES.has(code) || RETRYABLE_PG_CODES.has(code)) return true;
    const msg = String(err.message || '').toLowerCase();
    return msg.includes('idle-session timeout')
        || msg.includes('terminating connection')
        || msg.includes('connection closed')
        || msg.includes('connection ended');
}

const lastSuccessAt = new WeakMap();

function withinBypassWindow(pool, windowMs) {
    if (windowMs <= 0) return false;
    const last = lastSuccessAt.get(pool);
    return last !== undefined && (Date.now() - last) < windowMs;
}

async function runWithRetry(connection, sql, values, logSql) {
    const midFlightRetries = Math.max(0, resolveNumber(process.env.PG_QUERY_RETRIES, 3));
    const retryDelayMs = resolveNumber(process.env.PG_QUERY_RETRY_DELAY, 200);
    const acquireTimeoutMs = Math.max(1000, resolveNumber(process.env.PG_ACQUIRE_TIMEOUT, ACQUIRE_TIMEOUT_DEFAULT_MS));
    const preValidate = String(process.env.PG_PRE_VALIDATE || 'true').trim().toLowerCase() !== 'false';
    const bypassWindowMs = Math.max(0, resolveNumber(process.env.PG_PRE_VALIDATE_INTERVAL, 3000));

    const acquireDeadline = Date.now() + acquireTimeoutMs;
    let acquireAttempt = 0;
    let midFlightAttempt = 0;
    let sqlLogged = false;

    for (;;) {
        try {
            let result;
            if (preValidate && !withinBypassWindow(connection, bypassWindowMs)) {
                let reserved;
                try {
                    const reserveBudget = Math.max(1, acquireDeadline - Date.now());
                    reserved = await raceAcquire(connection.reserve(), reserveBudget, 'RESERVE_TIMEOUT',
                        (lateConn) => { try { lateConn.release(); } catch (_) {} });
                    logger.trace('Pre-validating connection (SELECT 1)');
                    await raceAcquire(reserved`SELECT 1`,
                        Math.min(VALIDATE_TIMEOUT_MS, Math.max(1, acquireDeadline - Date.now())), 'VALIDATE_TIMEOUT',
                        () => { try { reserved.release(); } catch (_) {} });
                } catch (acqErr) {
                    const acqCode = errCode(acqErr);
                    // on VALIDATE_TIMEOUT the ping is still in flight; the late-settle hook releases it
                    if (reserved && acqCode !== 'VALIDATE_TIMEOUT') {
                        try { reserved.release(); } catch (releaseErr) { logger.trace(`Failed to release stale connection :: ${releaseErr}`); }
                    }
                    logger.warn(`Pre-validation caught stale connection (${acqCode})`);
                    acqErr.preValidationCatch = true;
                    throw acqErr;
                }
                try {
                    if (logSql && !sqlLogged) { sqlLogged = true; logger.debug('Performing SQL Query'); logger.trace(`SQL Query :: ${sql}`); }
                    result = await reserved.unsafe(sql, values);
                } finally {
                    try { reserved.release(); } catch (releaseErr) {
                        logger.trace(`Failed to release reserved connection :: ${releaseErr}`);
                    }
                }
            } else {
                if (logSql && !sqlLogged) { sqlLogged = true; logger.debug('Performing SQL Query'); logger.trace(`SQL Query :: ${sql}`); }
                result = await connection.unsafe(sql, values);
            }
            if (preValidate) lastSuccessAt.set(connection, Date.now());
            const retried = acquireAttempt + midFlightAttempt;
            if (retried > 0) logger.warn(`Query recovered after ${retried} retr${retried === 1 ? 'y' : 'ies'} (stale connection absorbed)`);
            return result;
        } catch (err) {
            const code = errCode(err);
            // classify acquisition failures first (RESERVE/VALIDATE_TIMEOUT aren't in the retryable sets)
            if (err.preValidationCatch) {
                const remaining = acquireDeadline - Date.now();
                if (remaining <= 0) {
                    logger.error(`Could not acquire a live connection within ${acquireTimeoutMs}ms — giving up (${code}) :: ${err}`);
                    throw err;
                }
                acquireAttempt++;
                const delay = Math.min(jitter(retryDelayMs * acquireAttempt), 1000, remaining);
                logger.warn(`Acquiring live connection (attempt ${acquireAttempt}, ${remaining}ms budget left); retry in ${delay}ms`);
                await sleep(delay);
            } else if (!isRetryableConnectionError(err)) {
                logger.error(`Query failed (non-retryable ${code}) :: ${err}`);
                throw err;
            } else {
                midFlightAttempt++;
                if (midFlightAttempt > midFlightRetries) {
                    logger.error(`Query failed after ${midFlightRetries} mid-flight retries — giving up (${code}) :: ${err}`);
                    throw err;
                }
                const delay = jitter(retryDelayMs * midFlightAttempt);
                logger.warn(`Transient connection error mid-query (${code}); retry ${midFlightAttempt}/${midFlightRetries} in ${delay}ms`);
                logger.trace(`Retrying query :: ${sql}`);
                await sleep(delay);
            }
        }
    }
}


const PG_URI = /^(postgres(?:ql)?:\/\/)(?:(.*)@)?([^/?]*)(\/[^?]*)?(\?.*)?$/i;
const RESERVED = /[@\/?#\s"<>\\^`{|}\[\]]/;
const VALID_ESCAPES = /^(?:[^%]|%[0-9A-Fa-f]{2})*$/;

function encodeComponent(value) {
    const alreadyEncoded = value.includes('%') && VALID_ESCAPES.test(value) && !RESERVED.test(value);
    return alreadyEncoded ? value : encodeURIComponent(value);
}

function normalizeConnectionString(connectionString) {
    const m = typeof connectionString === 'string' && connectionString.match(PG_URI);
    if (!m) return connectionString;
    const [, scheme, userinfo, hosts, database = '', query = ''] = m;
    let credentials = '';
    if (userinfo !== undefined) {
        const colon = userinfo.indexOf(':');
        credentials = colon < 0
            ? encodeComponent(userinfo)
            : encodeComponent(userinfo.slice(0, colon)) + ':' + encodeComponent(userinfo.slice(colon + 1));
        credentials += '@';
    }
    return scheme + credentials + hosts + (database && '/' + encodeComponent(database.slice(1))) + query;
}

// one postgres.js pool per target for CRUDs with sharedPool: true or PG_SHARED_POOL=true; closed by the last user
const sharedPools = new Map(); // key -> { connection, users }

function sharedPoolKey(cd, baseOptions) {
    const target = cd.connectionString
        ? normalizeConnectionString(cd.connectionString)
        : `${cd.host}:${cd.port}/${cd.database}@${cd.user}`;
    const { max, prepare, idle_timeout, max_lifetime, connect_timeout, connection } = baseOptions;
    return JSON.stringify([target, !!cd.ssl, cd.parseDates, max, prepare, idle_timeout, max_lifetime, connect_timeout, connection]);
}

function sharedPoolUsers(key) {
    const pool = sharedPools.get(key);
    return pool ? pool.users : 0;
}

/**
 * @param {object} options CRUD options
 */
function CRUD(options) {
    this.database = options.database;
    this.customId = options.customId || false;
    this.idPattern = options.idPattern || '';

    this.connectionDetails = Object.fromEntries(
        Object.entries(options).filter(([_, v]) => v !== null && v !== undefined)
    );
}


/**
 * Connect using postgres library
 */
CRUD.prototype.connect = async function () {
    try {
        logger.debug('Connecting to PostgreSQL (postgres lib)');
        logger.trace(`Connection details :: ${JSON.stringify(this.connectionDetails)}`);

        const cd = this.connectionDetails;

        const optNum = (v) => { const n = parseInt(v, 10); return Number.isNaN(n) ? undefined : n; };

        const statementTimeout = String(cd.statementTimeout || process.env.PG_STATEMENT_TIMEOUT || '').trim();
        const useStatementTimeout = /^\d+\s*(ms|s|min|h|d)?$/i.test(statementTimeout) && parseInt(statementTimeout, 10) > 0;

        let baseOptions = {
            ssl: cd.ssl,
            max: parseInt(cd.maxPool, 10) || 10,
            prepare: String(cd.prepare) === 'false' ? false : undefined,
            idle_timeout: optNum(cd.idleTimeout),
            max_lifetime: optNum(cd.maxLifetime),
            connect_timeout: optNum(cd.connectTimeout),
            connection: Object.assign(
                { application_name: cd.applicationName || process.env.HOSTNAME || 'rest-crud' },
                useStatementTimeout ? { statement_timeout: statementTimeout } : {},
                cd.searchPath ? { search_path: cd.searchPath } : {}
            ),
            // postgres.js default reconnect backoff grows to 20s; cap it at 2s
            backoff: (retries) => Math.min(0.1 * (2 ** retries), 2) * (0.5 + Math.random() / 2),
            onnotice: (notice) => logger.trace(`PG Notice :: ${notice && notice.message}`),
            types: cd.parseDates === 'native' ? undefined : {
                date: {
                    from: [1082],
                    parse: v => v
                },
                timestamp: {
                    from: [1114],
                    parse: v => v
                },
                timestamptz: {
                    from: [1184],
                    parse: v => v
                }
            }
        };
        baseOptions = Object.fromEntries(
            Object.entries(baseOptions).filter(([_, v]) => v !== null && v !== undefined)
        );
        const shareable = String(cd.sharedPool !== undefined ? cd.sharedPool : process.env.PG_SHARED_POOL || 'false').trim().toLowerCase() === 'true';
        this.sharedPoolKey = shareable ? sharedPoolKey(cd, baseOptions) : null;
        const shared = this.sharedPoolKey ? sharedPools.get(this.sharedPoolKey) : null;
        if (shared) {
            shared.users++;
            this.connection = shared.connection;
            logger.info(`Joined shared PostgreSQL pool :: ${shared.users} users`);
        } else {
            this.connection = cd.connectionString
                ? postgres(normalizeConnectionString(cd.connectionString),
                    {
                        ...baseOptions
                    })
                : postgres({
                    host: cd.host,
                    port: cd.port,
                    username: cd.user,
                    password: cd.password,
                    database: cd.database,
                    ...baseOptions
                });
            if (this.sharedPoolKey) sharedPools.set(this.sharedPoolKey, { connection: this.connection, users: 1 });
        }

        if (!cd.lazyConnect) await this.connection`SELECT 1 + 1 AS solution`;

        const preValidate = String(process.env.PG_PRE_VALIDATE || 'true').trim().toLowerCase() !== 'false';
        const bypassWindowMs = Math.max(0, resolveNumber(process.env.PG_PRE_VALIDATE_INTERVAL, 3000));
        const connected = cd.lazyConnect ? 'PostgreSQL client ready, connects on first statement' : 'Connected to PostgreSQL';
        logger[cd.lazyConnect ? 'debug' : 'info'](`${connected} :: pool max=${baseOptions.max}${this.sharedPoolKey ? ` shared by ${sharedPoolUsers(this.sharedPoolKey)}` : ''}, application_name=${baseOptions.connection.application_name}`
            + ` | statement_timeout=${useStatementTimeout ? statementTimeout : 'disabled'}`);
        logger.debug(`Pre-validation ${preValidate ? 'ENABLED' : 'DISABLED'} (PG_PRE_VALIDATE)`
            + ` | bypass window=${bypassWindowMs}ms (PG_PRE_VALIDATE_INTERVAL${bypassWindowMs === 0 ? ' — every query validated' : ''})`
            + ` | request deadline=${DATASERVICE_TIMEOUT_MS}ms (DATASERVICE_TIMEOUT)`
            + ` | acquire budget=${Math.max(1000, resolveNumber(process.env.PG_ACQUIRE_TIMEOUT, ACQUIRE_TIMEOUT_DEFAULT_MS))}ms (PG_ACQUIRE_TIMEOUT)`
            + ` | mid-flight retries=${Math.max(0, resolveNumber(process.env.PG_QUERY_RETRIES, 3))} (PG_QUERY_RETRIES)`
            + ` | retry backoff=${resolveNumber(process.env.PG_QUERY_RETRY_DELAY, 200)}ms jittered (PG_QUERY_RETRY_DELAY)`);

        return 'Connection Successful';
    } catch (err) {
        logger.error('Error connecting :: ', err);
        throw err;
    }
};


/**
 * Disconnect postgres
 */
CRUD.prototype.disconnect = async function () {
    try {
        const shared = this.sharedPoolKey ? sharedPools.get(this.sharedPoolKey) : null;
        if (shared && shared.connection === this.connection) {
            shared.users--;
            if (shared.users > 0) {
                logger.info(`Left shared PostgreSQL pool :: ${shared.users} users remain`);
                return 'Database Disconnected';
            }
            sharedPools.delete(this.sharedPoolKey);
        }
        await this.connection.end({ timeout: 5 });
        logger.info('Database disconnected');
        return 'Database Disconnected';
    } catch (err) {
        logger.error('Error disconnecting :: ', err);
        throw err;
    }
};


/**
 * Raw SQL query handler
 */
CRUD.prototype.sqlQuery = async function (sql, values) {
    if (!sql) throw new Error('No sql query provided.');

    const result = await runWithRetry(this.connection, sql, Array.isArray(values) ? values : undefined, true);
    logger.trace(`Query result :: ${JSON.stringify(result[0])}`);
    return result;
};


/**
 * Runs fn in a transaction. Connection errors retry the whole block; fn must be safe to re-run.
 */
CRUD.prototype.withTransaction = async function (fn) {
    const maxRetries = Math.max(0, resolveNumber(process.env.PG_QUERY_RETRIES, 3));
    const retryDelayMs = resolveNumber(process.env.PG_QUERY_RETRY_DELAY, 200);

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            const result = await this.connection.begin(async (txSql) => {
                const tx = {
                    sqlQuery: (sql, values) => {
                        logger.trace(`TX SQL :: ${sql}`);
                        return txSql.unsafe(sql, values);
                    }
                };
                return fn(tx);
            });
            if (attempt > 0) logger.warn(`Transaction recovered after ${attempt} retr${attempt === 1 ? 'y' : 'ies'} (stale connection absorbed)`);
            return result;
        } catch (err) {
            if (!isRetryableConnectionError(err) || attempt >= maxRetries) {
                logger.error(`Error in transaction :: ${err}`);
                throw err;
            }
            const delay = jitter(retryDelayMs * (attempt + 1));
            logger.warn(`Transient connection error in transaction (${errCode(err)}); retry ${attempt + 1}/${maxRetries} as a whole block in ${delay}ms`);
            await sleep(delay);
        }
    }
};


/**
 * Table wrapper
 */
CRUD.prototype.table = function (table, jsonSchema) {
    return new Table({
        table,
        database: this.database,
        customId: this.customId,
        idPattern: this.idPattern,
        connection: this.connection
    }, jsonSchema);
};



/******************* TABLE CLASS ********************/

function Table(options, jsonSchema) {
    this.database = options.database;
    this.customId = options.customId;
    this.idPattern = options.idPattern;
    this.table = options.table;
    this.connection = options.connection;
    this.fields = utils.getFieldsFromSchema(jsonSchema);
}


/**
 * Create table
 */
Table.prototype.createTable = async function () {
    try {
        logger.debug(`Creating Table :: ${this.table}`);

        const sql = utils.createTableStatement(this.fields);
        const query = `CREATE TABLE IF NOT EXISTS ${this.table}(${sql})`;

        logger.trace(`SQL :: ${query}`);

        await runWithRetry(this.connection, query);

        logger.debug('Table created successfully');
        return 'Table created';
    } catch (err) {
        logger.error(`Error creating table :: ${err}`);
        throw err;
    }
};


/**
 * Count rows
 */
Table.prototype.count = async function (filter) {
    try {
        logger.debug('Counting rows in DB');
        let sql = `SELECT count(*) AS count FROM ${this.table}`;

        const whereClause = utils.whereClause(this.fields, filter);
        if (whereClause) sql += whereClause;

        logger.trace(`SQL :: ${sql}`);

        const result = await runWithRetry(this.connection, sql);
        return result[0].count;

    } catch (err) {
        logger.error(`Error counting records :: ${err}`);
        throw err;
    }
};


/**
 * List rows
 */
Table.prototype.list = async function (options) {
    try {
        logger.debug('Listing rows from DB');

        const whereClause = options?.filter ? utils.whereClause(this.fields, options.filter) : '';
        const selectClause = utils.selectClause(this.fields, options?.select) || '*';
        const limitClause = utils.limitClause(options?.count, options?.page);
        const orderByClause = utils.orderByClause(this.fields, options?.sort);

        let sql = `SELECT ${selectClause} FROM ${this.table}`;
        if (whereClause) sql += whereClause;
        if (orderByClause) sql += orderByClause;
        if (limitClause) sql += limitClause;

        logger.trace(`SQL :: ${sql}`);

        const result = await runWithRetry(this.connection, sql);
        return result;

    } catch (err) {
        logger.error(`Error listing records :: ${err}`);
        throw err;
    }
};


/**
 * Show a record
 */
Table.prototype.show = async function (id, options) {
    try {
        logger.debug(`Fetching record :: ${id}`);

        const selectClause = utils.selectClause(this.fields, options?.select) || '*';
        const sql = `SELECT ${selectClause} FROM ${this.table} WHERE _id='${id}'`;

        logger.trace(`SQL :: ${sql}`);

        const result = await runWithRetry(this.connection, sql);
        return result[0];

    } catch (err) {
        logger.error(`Error fetching record :: ${err}`);
        throw err;
    }
};


/**
 * Create records
 */
Table.prototype.create = async function (data) {
    try {
        logger.debug('Creating new rows');

        if (!data) throw new Error('No data to insert');
        if (!Array.isArray(data)) data = [data];

        data.forEach(obj => { if (!obj._id) obj._id = utils.token(); });

        const stmt = utils.insertManyStatement(this.fields, data);
        if (!stmt) throw new Error('No data to insert');

        const insertSQL = `INSERT INTO ${this.table} ${stmt} RETURNING *`;
        logger.trace(`SQL Insert :: ${insertSQL}`);

        const result = await runWithRetry(this.connection, insertSQL);
        return result;

    } catch (err) {
        logger.error(`Error in create :: ${err}`);
        throw err;
    }
};


/**
 * Update
 */
Table.prototype.update = async function (id, data) {
    try {
        logger.debug(`Updating record :: ${id}`);
        if (!id) throw new Error('No id provided');

        const stmt = utils.updateStatement(this.fields, data);
        if (!stmt) throw new Error('Invalid update payload');

        const sql =
            `UPDATE ${this.table} ${stmt} WHERE _id IN (${id.split(',').map(i => `'${i}'`).join(',')}) RETURNING _id`;

        logger.trace(`SQL :: ${sql}`);

        const result = await runWithRetry(this.connection, sql);
        return result.length;

    } catch (err) {
        logger.error(`Error updating :: ${err}`);
        throw err;
    }
};


/**
 * Delete
 */
Table.prototype.delete = async function (id) {
    try {
        logger.debug(`Deleting record :: ${id}`);
        if (!id) throw new Error('No id provided');

        const sql = `DELETE FROM ${this.table} WHERE _id='${id}' RETURNING _id`;
        logger.trace(`SQL :: ${sql}`);

        const result = await runWithRetry(this.connection, sql);
        return result.length;

    } catch (err) {
        logger.error(`Error deleting :: ${err}`);
        throw err;
    }
};


/**
 * Delete Many
 */
Table.prototype.deleteMany = async function (ids) {
    try {
        logger.debug(`Deleting multiple :: ${ids}`);
        if (!ids) throw new Error('No ids provided');

        const sql =
            `DELETE FROM ${this.table} WHERE _id IN (${ids.split(',').map(id => `'${id}'`).join(',')}) RETURNING _id`;

        logger.trace(`SQL :: ${sql}`);

        const result = await runWithRetry(this.connection, sql);
        return result.length;

    } catch (err) {
        logger.error(`Error deleting multiple :: ${err}`);
        throw err;
    }
};


module.exports = CRUD;
module.exports.normalizeConnectionString = normalizeConnectionString;
module.exports.sharedPoolCount = () => sharedPools.size;
