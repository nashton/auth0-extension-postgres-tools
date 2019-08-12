import * as uuid from 'uuid';
const ArgumentError = require('auth0-extension-tools').ArgumentError;
const NotFoundError = require('auth0-extension-tools').NotFoundError;
const ValidationError = require('auth0-extension-tools').ValidationError;

import { Pool, QueryResult } from 'pg';

export class PostgresRecordProvider {
  private pool: Pool;

  /**
   * Create a new PostgresRecordProvider.
   * @param {string} connectionString The connection string.
   * @constructor
   */
  constructor(connectionString: string) {
    if (connectionString === null || connectionString === undefined) {
      throw new ArgumentError('Must provide a connectionString');
    }
  
    if (typeof connectionString !== 'string') {
      throw new ArgumentError('The provided connectionString is invalid: ' + connectionString);
    }

    this.pool = new Pool({ connectionString });
  }

  /**
   * Get all records for a collection.
   * @param {string} collectionName The name of the collection.
   * @return {Array} The records.
   */
  public async getAll(collectionName: string) {
    const result = await this.pool.query(`SELECT json FROM ${collectionName}`)
    .catch(emptyOnMissingTable);

    return result.rows.map(row => row.json);
  }

  /**
   * Get a single record from a collection.
   * @param {string} collectionName The name of the collection.
   * @param {string} identifier The (uuid) identifier of the record.
   * @return {Object} The record.
   */
  public async get(collectionName: string, identifier: string) {
    const result = await this.pool.query(`SELECT json FROM ${collectionName} WHERE _id = $1`, [ identifier ])
    .catch(emptyOnMissingTable);

    if (result.rowCount === 0) {
      throw new NotFoundError(`The record ${identifier} in ${collectionName} does not exist.`);
    }
    return result.rows[0].json;
  }

  /**
   * Create a record in a collection.
   * @param {string} collectionName The name of the collection.
   * @param {Object} record The record.
   * @return {Object} The record.
   */
  public async create(collectionName: string, record: any) {
    if (!record._id) {
      record._id = uuid.v4();
    }

    try {
      const query = () => this.pool.query(`INSERT INTO ${collectionName} (json, _id) VALUES ($1, $2)`, [ JSON.stringify(record), record._id ]);
      await query().catch(ex => this.createOnMissingTable(ex, collectionName, query));
    } catch (ex) {
      if(isAlreadyExistsError(ex)) {
        throw new ValidationError('The record ' + record._id + ' in ' + collectionName + ' already exists.');
      }
      throw ex;
    }
    return record;
  }

  /**
   * Update a record in a collection.
   * @param {string} collectionName The name of the collection.
   * @param {string} identifier The identifier of the record to update.
   * @param {Object} record The record.
   * @param {boolean} upsert Flag allowing to upsert if the record does not exist.
   * @return {Object} The record.
   */
  public async update(collectionName: string, identifier: string, record: any, upsert: boolean = false) {
    record._id = identifier;

    if (upsert) {
      const query = () => this.pool.query(`INSERT INTO ${collectionName} (json, _id) VALUES ($1, $2) ON CONFLICT ON CONSTRAINT ${collectionName}__id_key DO UPDATE SET json = $1`, [ JSON.stringify(record), record._id ]);
      await query().catch(ex => this.createOnMissingTable(ex, collectionName, query));
    } else {
      const result = await this.pool.query(`UPDATE ${collectionName} SET json = $1 WHERE _id = $2`, [ JSON.stringify(record), record._id ])
      .catch(emptyOnMissingTable);

      if(result.rowCount === 0) {
        throw new NotFoundError(`The record ${identifier} in ${collectionName} does not exist.`);
      }
    }

    return record;
  }

  /**
   * Get all roles and groups associated with user
   * @param userId ID of the user
   * @returns JSON object with a groups and a roles array
   */
  public async getUserData(userId: string) {
    const groups = await this.pool.query(`SELECT json ->> '_id' AS id, json ->> 'name' AS name, json ->> 'roles' as roles FROM groups WHERE json->'members' @> $1`, [`"${userId}"`]);

    // get roles id
    const rolesIds = groups.rows.map(row => JSON.parse(row.roles) as string[])
      .reduce((prev, cur) => prev.concat(cur), [] as string[]); // Flatten array of arrays to one array with ids

    const roles = await this.pool.query(`SELECT json ->> 'name' AS role, json ->> 'permissions' AS permissions FROM roles WHERE json->'users' @> $1 OR _id = ANY ($2)`, [`"${userId}"`, rolesIds]);

    const permissionIds = roles.rows.map(row => JSON.parse(row.permissions) as string[])
      .reduce((prev, cur) => prev.concat(cur), [] as string[]); // Flatten array of arrays to one array with ids

    const permissions = await this.pool.query(`SELECT json ->> 'name' AS name FROM permissions WHERE _id = ANY ($1)`, [permissionIds]);

    return {
      groups: groups.rows.map(row => { return {
        _id: row.id,
        name: row.name
      }}),
      roles: roles.rows.map(row => row.role),
      permissions: permissions.rows.map(row => row.name)
    };
  }

  /**
   * Delete a record in a collection.
   * @param collectionName The name of the collection.
   * @param identifier The identifier of the record to update.
   */
  public async delete(collectionName: string, identifier: string) {
    await this.pool.query(`DELETE FROM ${collectionName} WHERE _id = $1`, [ identifier ]);
  };

    /**
   * Delete all collections.
   */
  public async deleteAll() {
    await this.pool.query(`DO $$ DECLARE
    r RECORD;
  BEGIN
    -- if the schema you operate on is not "current", you will want to
    -- replace current_schema() in query with 'schematodeletetablesfrom'
    -- *and* update the generate 'DROP...' accordingly.
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
  END $$;`);
  }

  /**
   * If missing table exception, create table and rerun query
   * @param ex 
   * @param collectionName 
   * @param query 
   */
  private async createOnMissingTable(ex: any, collectionName: string, query: () => Promise<QueryResult>) {
    if (!isMissingTableError(ex)) {
      throw ex;
    }

    try {
      await this.pool.query(`CREATE TABLE IF NOT EXISTS ${collectionName} (ID serial NOT NULL PRIMARY KEY, _id uuid NOT NULL UNIQUE, json jsonb NOT NULL)`);
    } catch (ex) {
      // Postgres create table command does not handle concurrency well and a race condition could happen.
      // This manifests as error 23505 which we therefore can safely ignore
      // https://www.postgresql.org/message-id/CA%2BTgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg%40mail.gmail.com
      if(ex.code === '23505') {
        console.warn(`Possible race condition error on creation of table ${collectionName}. It should be safe to continue.`)
      } else {
        throw ex;
      }
    }
    return query();
  }

  /**
   * Close the connection to the database.
   */
  public async closeConnection() {
    await this.pool.end();
  };
}

/**
 * If missing table exception, return empty result set
 * @param ex 
 */
function emptyOnMissingTable(ex: any): QueryResult {
  if (!isMissingTableError(ex)) {
    throw ex;
  }

  return {rowCount: 0, rows: [], command: '', oid: 0, fields: []};
}

function isMissingTableError(ex: any) {
  return ex.code === '42P01';
}

function isAlreadyExistsError(ex: any) {
  return ex.code === '23505';
}