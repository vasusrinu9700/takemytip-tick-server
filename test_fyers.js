const { fyersModel } = require('fyers-api-v3');
const fyers = new fyersModel();
console.log('fyers instance properties:', Object.keys(fyers));
console.log('fyers methods in proto:', Object.getOwnPropertyNames(Object.getPrototypeOf(fyers)));
console.log('fyers stringified:', fyers.toString());
console.log('fyers getQuotes type:', typeof fyers.getQuotes);
