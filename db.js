const mysql = require("mysql2/promise");
const { db } = require("./config");

const pool = mysql.createPool(db);

async function checkConnection() {
  try {
    const connection = await pool.getConnection();
    console.log("✅ Database connected successfully");
    connection.release(); // release back to pool
  } catch (err) {
    console.error("❌ Database connection failed:", err.message);
  }
}

checkConnection();

module.exports = pool;
