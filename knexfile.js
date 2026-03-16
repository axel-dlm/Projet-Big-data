// Configuration Knex.js pour la connexion PostgreSQL (Neon)
require('dotenv').config();

module.exports = {
  client: 'pg',
  connection: process.env.DATABASE_URL,
  pool: {
    min: 2,
    max: 10
  },
  // Désactive le SSL auto-signé pour Neon
  ssl: {
    rejectUnauthorized: false
  }
};
