require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool(); // This will use the environment variables from .env

async function createNormalizedTables() {
    try {
        // Drop existing tables and constraints
        await pool.query(`
            ALTER TABLE IF EXISTS codex_entries 
            DROP COLUMN IF EXISTS species_id,
            DROP COLUMN IF EXISTS system_id,
            DROP COLUMN IF EXISTS body_id;

            DROP TABLE IF EXISTS bodies CASCADE;
            DROP TABLE IF EXISTS systems CASCADE;
            DROP TABLE IF EXISTS species CASCADE;
            DROP TABLE IF EXISTS regions CASCADE;
        `);

        // Create tables without foreign key constraints first
        await pool.query(`
            -- Create species table
            CREATE TABLE species (
                id SERIAL PRIMARY KEY,
                english_name VARCHAR(255) UNIQUE NOT NULL
            );

            -- Create regions table
            CREATE TABLE regions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                name_localised VARCHAR(255)
            );

            -- Create systems table
            CREATE TABLE systems (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                x DECIMAL,
                y DECIMAL,
                z DECIMAL,
                region_id INTEGER REFERENCES regions(id)
            );

            -- Create bodies table
            CREATE TABLE bodies (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                system_id INTEGER REFERENCES systems(id),
                UNIQUE(name, system_id)
            );

            -- Add foreign key columns to codex_entries
            ALTER TABLE codex_entries 
            ADD COLUMN species_id INTEGER REFERENCES species(id),
            ADD COLUMN system_id INTEGER REFERENCES systems(id),
            ADD COLUMN body_id INTEGER REFERENCES bodies(id);
        `);

        console.log('Normalized tables created successfully');
    } catch (err) {
        console.error('Error creating normalized tables:', err);
        throw err;
    }
}

async function normalizeData() {
    try {
        // Check if required columns exist before proceeding
        const columnCheck = await pool.query(`
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = 'codex_entries' AND column_name = 'english_name'
            ) as has_english_name,
            EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = 'codex_entries' AND column_name = 'region_name'
            ) as has_region_name,
            EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = 'codex_entries' AND column_name = 'region_name_localised'
            ) as has_region_name_localised,
            EXISTS (
                SELECT 1 
                FROM information_schema.columns 
                WHERE table_name = 'codex_entries' AND column_name = 'system'
            ) as has_system;
        `);

        const {
            has_english_name,
            has_region_name,
            has_region_name_localised,
            has_system
        } = columnCheck.rows[0];

        // Insert unique species if english_name column exists
        if (has_english_name) {
            await pool.query(`
                INSERT INTO species (english_name)
                SELECT DISTINCT english_name 
                FROM codex_entries 
                WHERE english_name IS NOT NULL
                ON CONFLICT (english_name) DO NOTHING;
            `);
            console.log('Species data normalized');
        } else {
            console.log('Skipping species normalization - english_name column not found');
        }

        // Insert unique regions if required columns exist
        if (has_region_name) {
            const regionQuery = has_region_name_localised
                ? `
                    INSERT INTO regions (name, name_localised)
                    SELECT DISTINCT region_name, region_name_localised
                    FROM codex_entries 
                    WHERE region_name IS NOT NULL
                    ON CONFLICT (name) DO NOTHING;
                `
                : `
                    INSERT INTO regions (name)
                    SELECT DISTINCT region_name
                    FROM codex_entries 
                    WHERE region_name IS NOT NULL
                    ON CONFLICT (name) DO NOTHING;
                `;
            
            await pool.query(regionQuery);
            console.log('Regions data normalized');
        } else {
            console.log('Skipping regions normalization - region_name column not found');
        }

        // Insert unique systems with their region references if system column exists
        if (has_system) {
            const systemQuery = has_region_name
                ? `
                    WITH system_data AS (
                        SELECT DISTINCT 
                            ce.system,
                            ce.x,
                            ce.y,
                            ce.z,
                            r.id as region_id
                        FROM codex_entries ce
                        LEFT JOIN regions r ON ce.region_name = r.name
                        WHERE ce.system IS NOT NULL
                    )
                    INSERT INTO systems (name, x, y, z, region_id)
                    SELECT * FROM system_data
                    ON CONFLICT (name) DO NOTHING;
                `
                : `
                    WITH system_data AS (
                        SELECT DISTINCT 
                            system,
                            x,
                            y,
                            z
                        FROM codex_entries
                        WHERE system IS NOT NULL
                    )
                    INSERT INTO systems (name, x, y, z)
                    SELECT * FROM system_data
                    ON CONFLICT (name) DO NOTHING;
                `;
            
            await pool.query(systemQuery);
            console.log('Systems data normalized');
        } else {
            console.log('Skipping systems normalization - system column not found');
        }

        // Insert unique bodies with their system references if system column exists
        if (has_system) {
            await pool.query(`
                WITH body_data AS (
                    SELECT DISTINCT 
                        ce.body,
                        s.id as system_id
                    FROM codex_entries ce
                    JOIN systems s ON ce.system = s.name
                    WHERE ce.body IS NOT NULL
                )
                INSERT INTO bodies (name, system_id)
                SELECT * FROM body_data
                ON CONFLICT (name, system_id) DO NOTHING;
            `);
            console.log('Body data normalized');
        } else {
            console.log('Skipping bodies normalization - system column not found');
        }

        // Update codex_entries with foreign keys based on available columns
        const updateParts = [];
        if (has_english_name) {
            updateParts.push('species_id = (SELECT id FROM species WHERE english_name = ce.english_name)');
        }
        if (has_system) {
            updateParts.push('system_id = (SELECT id FROM systems WHERE name = ce.system)');
            updateParts.push(`
                body_id = (
                    SELECT b.id 
                    FROM bodies b
                    JOIN systems s ON b.system_id = s.id
                    WHERE b.name = ce.body AND s.name = ce.system
                )
            `);
        }

        const whereConditions = [];
        if (has_english_name) {
            whereConditions.push('ce.english_name IS NOT NULL');
        }
        if (has_system) {
            whereConditions.push('ce.system IS NOT NULL OR ce.body IS NOT NULL');
        }

        if (updateParts.length > 0) {
            await pool.query(`
                UPDATE codex_entries ce
                SET ${updateParts.join(', ')}
                WHERE ${whereConditions.join(' OR ')};
            `);
            console.log('Foreign keys updated in codex_entries');
        }

    } catch (err) {
        console.error('Error normalizing data:', err);
        throw err;
    }
}

async function cleanUpColumns() {
    try {
        await pool.query(`
            ALTER TABLE codex_entries
            DROP COLUMN IF EXISTS index_id,
            DROP COLUMN IF EXISTS hud_category,
            DROP COLUMN IF EXISTS name,
            DROP COLUMN IF EXISTS system,
            DROP COLUMN IF EXISTS x,
            DROP COLUMN IF EXISTS y,
            DROP COLUMN IF EXISTS z,
            DROP COLUMN IF EXISTS body,
            DROP COLUMN IF EXISTS name_localized,
            DROP COLUMN IF EXISTS category_localized,
            DROP COLUMN IF EXISTS region_name,
            DROP COLUMN IF EXISTS region_name_localised;
        `);
        console.log('Successfully removed unnecessary columns from codex_entries table');
    } catch (error) {
        console.error('Error cleaning up columns:', error);
        throw error;
    }
}

async function main() {
    try {
        await createNormalizedTables();
        await normalizeData();
        await cleanUpColumns();
        console.log('Database normalization completed successfully');
    } catch (error) {
        console.error('Error in main:', error);
        process.exit(1);
    } finally {
        await pool.end();
    }
}

main();
