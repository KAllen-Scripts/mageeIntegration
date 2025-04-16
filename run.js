let common = require('./common.js');

let failedArr = [];

(async ()=>{

    // await require('./scripts/UOM.js').run()
    // await require('./scripts/POs.js').run()
    try{await require('./scripts/itemsRedo.js').run()}catch{failedArr.push('./scripts/itemsRedo.js')}
    try{await require('./scripts/BOMS.js').run()}catch{failedArr.push('./scripts/BOMS.js')}
    try{await require('./scripts/manufacturing.js').run()}catch{failedArr.push('./scripts/manufacturing.js')}
    try{await require('./scripts/makeBins.js').run()}catch{failedArr.push('./scripts/makeBins.js')}
    try{await require('./scripts/vouchers.js').run()}catch{failedArr.push('./scripts/vouchers.js')}
    try{await require('./scripts/saleOrders.js').run()}catch{failedArr.push('./scripts/saleOrders.js')}
    
})()