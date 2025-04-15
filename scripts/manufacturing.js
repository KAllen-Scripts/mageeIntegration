let common = require('../common.js');
const convertCSV = require("json-2-csv");
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);
let manufacturingSheet = '../files - new/order/SO/SO_20241216155051.csv';
let supplierSheet = '../files - new/sage stuff/suppliers.csv';

let items = {}
let locations = {}
let manufacturers = {}
let suppliers = {}
let existingSuppliers = {}
let saleOrders = {}
let existingRuns = {}
let manufacturingCosts = {}

let receipts = [];

let warehouseLocation = '470de3f4-feeb-4a19-9b29-bce3a4218319'

async function getOrdersSheet(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(manufacturingSheet)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()

                if (row['product order status'].toLowerCase() != 'cancelled'){
                    let missing = false
                    if (saleOrders[row['order number']] == undefined){saleOrders[row['order number']] = {
                        customer: parseInt(row['supplier code']),
                        createdDate: new Date(row['order create date'].replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3')).toISOString(),
                        shipmentDate: new Date(row['shipment date'].replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3')).toISOString(),
                        deliveryDate: new Date(row['delivery date'].replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3')).toISOString(),
                        requestDate: new Date(row['request date'].replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3')).toISOString(),
                        items:[],
                        type: row['order type name'],
                        number: row['order number'],
                        ref:  row['order reference'],
                        curency: row['order currency'],
                        exampleBaseCurrency: row['order value in base currency'],
                        exampleOrderCurrency: row['order value in order currency'],
                        supplier: row['supplier name']
                    }}
                    if (items[row['sku code'].toLowerCase()] == undefined){
                        missing = true
                    }
                    saleOrders[row['order number']].items.push({...row, missing})
                }

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function getCustomersSheet(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(supplierSheet)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()

                let customerCode
                isNaN(row['code']) ? customerCode = row['code'] : customerCode = parseInt(row['code'])
                suppliers[customerCode] = row

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function getAllRuns(){
    await common.loopThrough('Getting Runs', `https://api.stok.ly/v0/manufacturing-runs`, 'size=1000&sortDirection=ASC&sortField=niceId', '', async (m)=>{
        if(m.reference == undefined){return}
        if(m.reference.split('||')[1] == undefined){return}
        existingRuns[m.reference.split('||')[1].trim()] = m.runId
    })
};

async function getAllManufacturers(){
    await common.loopThrough('Getting Manufacturers', `https://api.stok.ly/v0/manufacturers`, 'size=1000', '[status]=={active}', async (m)=>{
        manufacturers[m.name.toLowerCase().trim()] = m.manufacturerId
    })
};

async function getAllManufactureringCosts(){
    await common.loopThrough('Getting Manufacturering costs', `https://api.stok.ly/v0/manufacturing-costs`, 'size=1000', '', async (m)=>{
        if(manufacturingCosts[m.itemId] == undefined){manufacturingCosts[m.itemId] = []}
        manufacturingCosts[m.itemId].push(m)
    })
};

async function getAllLocations(){
    await common.loopThrough('Getting Locations', `https://api.stok.ly/v0/locations`, 'size=1000', '[status]=={0}', async (location)=>{
        locations[location.name.toLowerCase().trim()] = location.locationId
    })
};

async function getAllSuppliers(){
    await common.loopThrough('Getting Suppliers', `https://api.stok.ly/v1/suppliers`, 'size=1000', '[status]!={1}', async (supplier)=>{
        existingSuppliers[supplier.name.toLowerCase().trim()] = supplier.supplierId
    })
};

async function getAllSimples(){
    await common.loopThrough('Getting Manufactured Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{3}))%26%26([status]!={1}))', async (item)=>{
        items[item.sku.toLowerCase().trim()] = item.itemId
    })
};

let problemOrders = []
async function processOrders(){
    let orderArr = []
    for (const order in saleOrders){
        if(!existingRuns.includes(order)){orderArr.push(saleOrders[order])}
    }
    orderArr.sort((a, b) => new Date(a.createdDate) - new Date(b.createdDate))
    
    for (const order of orderArr){
        try{
            let orderReceipts = {}
            let problemOrder = false
            let contacts = []
            if (suppliers[order.customer]['contact email'] != '' || suppliers[order.customer]['contact telephone'] != ''){
                contacts = [{
                    "forename": suppliers[order.customer]['short name'],
                    "surname": "",
                    "email": suppliers[order.customer]['contact email'],
                    "phone": suppliers[order.customer]['contact telephone'],
                    "tags": [],
                    "name": {
                        "forename": suppliers[order.customer]['short name'],
                        "surname": ""
                    }
                }]
            }



            if (locations[suppliers[order.customer].name.toLowerCase().trim() + ' - ' + suppliers[order.customer].code] == undefined){
                await common.requester('post', `https://api.stok.ly/v0/locations`, {
                    "name": suppliers[order.customer].name + ' - ' + suppliers[order.customer].code,
                    "address": {
                        "line1": suppliers[order.customer]['address line1'].length > 2 ? suppliers[order.customer]['address line1'] : 'undefined',
                        "line2": suppliers[order.customer]['address line2'] > 2 ? suppliers[order.customer]['address line2'] : '',
                        "city": suppliers[order.customer]['city'] > 2 ? suppliers[order.customer]['city'] : 'undefined',
                        "region": suppliers[order.customer]['county'] > 2 ? suppliers[order.customer]['county'] : '',
                        "country": suppliers[order.customer]['country code'],
                        "postcode": "undefined"
                    },
                    "contacts": contacts
                }).then(r=>{
                    locations[suppliers[order.customer].name.toLowerCase() + ' - ' + suppliers[order.customer].code] = r.data.data.id
                })
            }


            if (manufacturers[suppliers[order.customer].name.toLowerCase().trim() + ' - ' + suppliers[order.customer].code] == undefined){
                await common.requester('post', `https://api.stok.ly/v0/manufacturers`, {
                    "name": suppliers[order.customer].name + ' - ' + suppliers[order.customer].code,
                    "accountReference": order['default nominal account number'],
                    "vatNumber": suppliers[order.customer]['tax registration number'] != '' ? {
                        "value": suppliers[order.customer]['tax registration number'],
                        "country": suppliers[order.customer]['country code']
                    } : undefined,
                    "currency": suppliers[order.customer]['contact email'].currency,
                    contacts: contacts,
                    "addresses": [
                        {
                            "line1": suppliers[order.customer]['address line1'].length > 2 ? suppliers[order.customer]['address line1'] : 'undefined',
                            "line2": suppliers[order.customer]['address line2'] > 2 ? suppliers[order.customer]['address line2'] : '',
                            "city": suppliers[order.customer]['city'] > 2 ? suppliers[order.customer]['city'] : 'undefined',
                            "region": suppliers[order.customer]['county'] > 2 ? suppliers[order.customer]['county'] : '',
                            "country": suppliers[order.customer]['country code'],
                            "postcode": "undefined"
                        }
                    ]
                }).then(r=>{
                    manufacturers[suppliers[order.customer].name.toLowerCase().trim() + ' - ' + suppliers[order.customer].code] = r.data.data.id
                })
            }

            let additionalCosts = []

            let orderJSON = {
                "manufacturerId": manufacturers[order.supplier.toLowerCase().trim() + ' - ' + suppliers[order.customer].code],
                "reference": `${order.ref} || ${order.number}`,
                "manufacturingLocationId": locations[order.supplier.toLowerCase().trim() + ' - ' + suppliers[order.customer].code],
                "deliveryLocationId": warehouseLocation,
                startExpectedAt: order.requestDate,
                deliveryExpectedAt: order.deliveryDate,
                items: []
            }


            for (const item of order.items){

                if (orderReceipts[item['delivery date']] == undefined){orderReceipts[item['delivery date']] = {items: [], date: item['delivery date']}}

                let itemSku = `${item['product code']}_${item['colour code']}-${item['size code']}`
                if (item['fit code'] != ''){itemSku += item['fit code']}
        
                console.log(items[itemSku.toLowerCase().trim()])
                console.log(itemSku.toLowerCase().trim())
                if (items[itemSku.toLowerCase().trim()] == undefined){
                    await common.requester('get', `https://api.stok.ly/v0/items?sortDirection=ASC&sortField=status&filter=(([sku]=={@string;${item['sku code']}})))`).then(async r=>{
                        if (r.data.data[0]?.itemId != undefined){
                            items[itemSku.toLowerCase().trim()] = r.data.data[0]?.itemId
                            return
                        }
                        problemOrder = true
                    })
                }

                orderJSON.items.push({
                    "itemId": items[itemSku.toLowerCase().trim()],
                    expectedDate: new Date(item['request date'].replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3')).toISOString(),
                    "quantityExpected": Math.max(parseInt(item['order quantity']), parseInt(item['order received quantity']))
                });

                orderReceipts[item['delivery date']].items.push({
                    "itemId": items[itemSku.toLowerCase().trim()],
                    "quantityExpected": Math.max(parseInt(item['order quantity']), parseInt(item['order received quantity'])),
                    "quantityReceived": parseInt(item['order received quantity']),
                    "quantityBlemished": 0,
                    "totalCost": {
                        "amount": parseFloat((parseFloat(item['order value in base currency']) / parseInt(item['order received quantity'])).toFixed(2)),
                        "currency": "EUR"
                    }
                })


                for (const a of (manufacturingCosts?.[items?.[itemSku.toLowerCase().trim()]] || [])){
                    let costFound = false
                    for (const i in additionalCosts){
                        if (additionalCosts[i].manufacturingCostId == a.getAllManufactureringCostsId){
                            additionalCosts[i].quantity += parseInt(item['order quantity'])
                            costFound = true
                        }
                    }
                    if (!costFound && a.automaticallyIncludeOnManufacturingRuns){
                        additionalCosts.push({
                            "name": a.name,
                            "quantity": parseInt(item['order quantity']),
                            "cost": a.cost,
                            "tax": a.tax,
                            "taxClassId": a.taxClassId,
                            "taxRate": a.rate,
                            manufacturingCostId: a.manufacturingCostId
                        })
                    }
                }
            }
            if(!problemOrder){
                try{
                    let runId = await common.requester('post', `https://api.stok.ly/v0/manufacturing-runs`, {...orderJSON, additionalCosts}, 0).then(r=>{return r.data.data.id})
                    for (const receipt of Object.keys(orderReceipts)){
                        receipts.push({
                            "locationId": warehouseLocation,
                            "items": orderReceipts[receipt].items,
                            "referenceType": 3,
                            "referenceIds": [
                                runId
                            ],
                            date: orderReceipts[receipt].date
                        })
                    }
                } catch (err) {
                    console.log(err)
                }
            }else{
                problemOrders.push(order.number)
            }
        } catch{}
    }
}

async function makeGoodsReceipts(){
    console.log(receipts)
    receipts.sort((a, b) => a.date - b.date);
    for (const receipt of receipts){
        // try{
            let receiptId;
            let makeReceiptAttemptsattempts = 0;
            const maxRetries = 10;
            const delayMs = 5000;
    
            while (makeReceiptAttemptsattempts < maxRetries) {
                try {
                    // Attempt the request
                    const response = await common.requester('post', `https://api.stok.ly/v0/goods-receipts`, receipt);
                    receiptId = response.data.data.id;
                    break; // Success, exit the loop
                } catch (error) {
                    makeReceiptAttemptsattempts++;
                    
                    if (makeReceiptAttemptsattempts >= maxRetries) {
                    // If we've reached max retries, throw the error
                    throw new Error(`Failed to create goods receipt after ${maxRetries} attempts: ${error.message}`);
                    }
                    
                    // Wait before the next retry
                    await new Promise(resolve => setTimeout(resolve, delayMs));
                }
            }
            
    
    
            let attempts = 0;
            const maxAttempts = 10;
            while (attempts < maxAttempts) {
                try {
                    await common.requester('post', `https://api.stok.ly/v0/goods-receipts/${receiptId}/completions`);
                    break; // Success - exit the loop
                } catch (error) {
                    attempts++;
                    console.log(`Attempt ${attempts} failed: ${error.message}`);
                    
                    if (attempts >= maxAttempts) {
                    console.log(`Max attempts (${maxAttempts}) reached. Moving on.`);
                    break; // Give up after max attempts
                    }
                    
                    // Wait for 2 seconds before next attempt
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }
        // } catch {}
    }
}

async function run(){
    await getAllManufactureringCosts();
    await getAllRuns();
    await getCustomersSheet();
    await getOrdersSheet();
    await getAllSuppliers();
    await getAllSimples();
    await getAllManufacturers();
    await getAllLocations();
    await processOrders();
    await makeGoodsReceipts()

    console.log(problemOrders)
}

module.exports = {
    run
}