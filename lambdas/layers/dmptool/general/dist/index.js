"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.currentDateAsString = void 0;
// Function to return the current date as "YYYY-MM-DD"
const currentDateAsString = () => {
    const now = new Date();
    // Extract components
    const year = now.getFullYear();
    // Months are 0-indexed so need to add 1 here
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    // Format as YYYY-MM-DD
    return `${year}-${month}-${day}`;
};
exports.currentDateAsString = currentDateAsString;
