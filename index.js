require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const JSONStream = require('JSONStream');

const pool = new Pool(); // This will use the environment variables from .env

async function createTable() {
    const createTableQuery = `
        CREATE TABLE IF NOT EXISTS codex_entries (
            id SERIAL PRIMARY KEY,
            english_name VARCHAR(255),
            created_at TIMESTAMP,
            reported_at TIMESTAMP,
            cmdrName VARCHAR(255),
            system VARCHAR(255),
            x DECIMAL,
            y DECIMAL,
            z DECIMAL,
            body VARCHAR(255),
            latitude DECIMAL,
            longitude DECIMAL,
            entryid INTEGER,
            name VARCHAR(255),
            category VARCHAR(255),
            sub_category VARCHAR(255),
            sub_category_localised VARCHAR(255),
            region_name VARCHAR(255),
            id64 BIGINT
        );
    `;

    try {
        await pool.query(createTableQuery);
        console.log('Table created successfully');
    } catch (err) {
        console.error('Error creating table:', err);
        throw err;
    }
}

async function insertData() {
    const insertQuery = `
        INSERT INTO codex_entries (
            english_name, created_at, reported_at,
            cmdrName, system, x, y, z, body, latitude, longitude, 
            entryid, name, category, sub_category, 
            sub_category_localised, region_name, id64
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                 $14, $15, $16, $17, $18)
    `;

    return new Promise((resolve, reject) => {
        const jsonStream = fs.createReadStream(path.join(__dirname, 'codex.json'))
            .pipe(JSONStream.parse('*'));

        let count = 0;
        const batchSize = 1000;
        let batch = [];

        jsonStream.on('data', async (entry) => {
            try {
                // Only process entries with hud_category === "Biology"
                if (entry.hud_category !== "Biology") {
                    return;
                }

                batch.push([
                    entry.english_name,
                    entry.created_at,
                    entry.reported_at,
                    entry.cmdrName,
                    entry.system,
                    parseFloat(entry.x),
                    parseFloat(entry.y),
                    parseFloat(entry.z),
                    entry.body,
                    parseFloat(entry.latitude),
                    parseFloat(entry.longitude),
                    entry.entryid,
                    entry.name,
                    entry.category,
                    entry.sub_category,
                    entry.sub_category_localised,
                    entry.region_name,
                    entry.id64
                ]);

                count++;

                if (batch.length >= batchSize) {
                    jsonStream.pause();
                    for (const item of batch) {
                        await pool.query(insertQuery, item);
                    }
                    batch = [];
                    jsonStream.resume();
                }

                if (count % batchSize === 0) {
                    console.log(`Processed ${count} entries`);
                }
            } catch (err) {
                jsonStream.destroy();
                reject(err);
            }
        });

        jsonStream.on('end', async () => {
            try {
                // Insert any remaining items
                for (const item of batch) {
                    await pool.query(insertQuery, item);
                }
                console.log(`Finished processing ${count} entries`);
                resolve();
            } catch (err) {
                reject(err);
            }
        });

        jsonStream.on('error', (err) => {
            reject(err);
        });
    });
}

async function main() {
    try {
        await createTable();
        await insertData();
        console.log('Data insertion completed successfully');
    } catch (err) {
        console.error('Error in main process:', err);
    } finally {
        await pool.end();
    }
}

main();