let common = require('./common.js');

let failedArr = [];

(async ()=>{
    // await require('./scripts/itemsRedo.js').run()
    // await require('./scripts/UOM.js').run()
    // await require('./scripts/BOMS.js').run()

    await require('./scripts/POs.js').run()
    // await require('./scripts/manufacturing.js').run()

    // await require('./scripts/saleOrders.js').run()
    // await require('./scripts/saleOrders.js').run()
    // await require('./scripts/saleOrders2.js').run()

    // try { await require('./scripts/makeBins.js').run() } catch { failedArr.push('./scripts/makeBins.js') }
    // try { await require('./scripts/vouchers.js').run() } catch { failedArr.push('./scripts/vouchers.js') }

    // await require('./scripts/inventory.js').run()
})()