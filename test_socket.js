const { fyersDataSocket } = require("fyers-api-v3");
const fs = require('fs');

console.log("🔍 Running WebSocket Connection Test locally...");

// Let's see if we can find credentials in system settings if there is a backup or config file.
// Otherwise, let's test with placeholder or ask the user to run it with their credentials.
const appId = "9FA94IEI7K-100";
const accessToken = process.argv[2] || "YOUR_ACCESS_TOKEN_HERE";

if (!accessToken || accessToken === "YOUR_ACCESS_TOKEN_HERE") {
  console.log("⚠️ Please run this script with your access token as an argument: node test_socket.js <YOUR_ACCESS_TOKEN>");
  process.exit(1);
}

const authString = `${appId}:${accessToken}`;
console.log(`🔌 Initializing official Fyers Data Socket (AppId: ${appId})`);

try {
  const fyersWs = new fyersDataSocket(authString, "./logs", true);

  fyersWs.on('connect', () => {
    console.log("✅ SUCCESS! Connected to Fyers Data WebSocket successfully!");
    
    // Subscribe to test index
    console.log("📡 Subscribing to NSE:NIFTY50-INDEX...");
    fyersWs.subscribe(["NSE:NIFTY50-INDEX"]);
  });

  fyersWs.on('message', (data) => {
    console.log("📥 RECEIVED MESSAGE:", JSON.stringify(data));
  });

  fyersWs.on('error', (err) => {
    console.error("🚨 ERROR RECEIVED:", err);
  });

  fyersWs.on('close', () => {
    console.log("❌ CONNECTION CLOSED");
  });

  fyersWs.connect();

  // Keep alive for 15 seconds
  setTimeout(() => {
    console.log("⏱️ Test complete. Closing socket.");
    try { fyersWs.close(); } catch(e){}
    process.exit(0);
  }, 15000);

} catch (err) {
  console.error("🚨 CRITICAL EXCEPTION:", err);
}
