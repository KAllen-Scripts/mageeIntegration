let common = require('../common.js');
const convertCSV = require("json-2-csv");
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);

let customerSheet = '../files/saleOrders/customers.csv';
let ordersSheetWeb = '../files/saleOrders/web.csv';
let ordersSheetAnns = '../files/saleOrders/anns.csv';


let customersFromSheet = {};
let items = {};
let saleOrders = {};
let customerList = {};
let orders = [];

async function getAllItems(){
    await common.loopThrough('Getting Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]!={2}))%26%26([status]!={1})', async (item)=>{
        items[item.sku.toLowerCase()] = item.itemId
    })
};

async function getAllOrders(){
    await common.loopThrough('Getting Orders', `https://api.stok.ly/v2/saleorders`, 'sortDirection=ASC&sortField=niceId', '(([sourceId]=*{38ed9f3e-85c6-4d1b-b963-44978c9ba10c}%26%26(([stage]=={order}%26%26[itemStatuses]::{unprocessed}))))%26%26([stage]!={removed})', async (order)=>{
        orders.push(order.sourceReferenceId)
    })
};

async function getAllCustomers(){
    await common.loopThrough('Getting Customers', `https://api.stok.ly/v0/customers`, 'size=1000&sortDirection=ASC&sortField=name', '[status]=={1}', async (cust)=>{
        customerList[cust.barcode] = cust.customerId
    })
};

async function getCustomersSheet(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(customerSheet)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()

                let customerCode
                isNaN(row['code']) ? customerCode = row['code'] : customerCode = parseInt(row['code'])
                customersFromSheet[customerCode] = row

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function getOrdersSheet(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(ordersSheetAnns)
        .pipe(csv.parse({headers: headers => headers.map(h => h.replace(/\s+/g, ' ').trim().toLowerCase())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()
                if (row['date'] != '' && !row['date'].includes('Total') && parseInt(row['qty sold']) > 0 && row['type'] != 'GV'){
                    
                    if(saleOrders[row['date']+'ann'] == undefined){
                        saleOrders[row['date']+'ann'] = {
                            date: new Date(row['date'].replace(/(\d{2})\/(\d{2})\/(\d{4})/, '$3-$2-$1')).toISOString(),
                            location: 'f2b4d12d-684b-4fe5-97c8-56719bce5294',
                            channel: '53373b61-4ea5-44c4-9577-fd5780bb9475',
                            barcode: row['date'],
                            items:[],
                        }
                    }
                    saleOrders[row['date']+'ann'].items.push(row)
                }

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}


async function getOrdersSheet2(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(ordersSheetWeb)
        .pipe(csv.parse({headers: headers => headers.map(h => h.replace(/\s+/g, ' ').trim().toLowerCase())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()
                if (row['date'] != '' && !row['date'].includes('Total') && parseInt(row['qty sold']) > 0 && row['type'] != 'GV'){
                    
                    if(saleOrders[row['date'] + '-web'] == undefined){
                        saleOrders[row['date'] + '-web'] = {
                            date: new Date(row['date'].replace(/(\d{2})\/(\d{2})\/(\d{4})/, '$3-$2-$1')).toISOString(),
                            location: 'e5638b41-0949-4183-91db-3b65c380eff5',
                            channel: '95d7ce86-7aa3-4c5d-8b7a-f5be1173b5dc',
                            barcode: row['date'] + 'web',
                            items:[],
                        }
                    }
                    saleOrders[row['date'] + '-web'].items.push(row)
                }

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function processOrders(){
    let orderArr = []
    for (const order in saleOrders){
        orderArr.push(saleOrders[order])
    }
    orderArr.sort((a, b) => new Date(a.date) - new Date(b.date))
    for (const order of orderArr){
        let orderJSON = {
            "stage": "order",
            "createdAt": order.date,
            "sourceType": "magento_two",
            sourceId: order.channel,
            sourceReferenceId: order.barcode,
            "currency": order.curency,
            barcode: order.barcode,
            "items": [],
            currency: 'GBP',
            payments: [{method: 4, amount: 0}]
        }



        
        for (const item of order.items){
            let refID

            if(items[[item['spc'], item['colour'], item['size']].join('-').toLowerCase().trim()] != undefined){
                refID = items[[item['spc'], item['colour'], item['size']].join('-').toLowerCase().trim()]
            } else {
                refID = items[[item['spc'].toLowerCase().trim()]]
            }

            if (refID == undefined){continue}

            orderJSON.items.push({
                "type": "item",
                "referenceId": refID,
                "displayPrice": parseFloat(item['sell value'] / item['qty sold']),
                "displayTax": parseFloat(item['vat amount'] / item['qty sold']),
                "quantity": parseInt(item['qty sold']),
                "displayLinePrice": parseFloat(item['sell value']),
                "displayLineTax": parseFloat(item['vat amount']),
                "displayLineDiscount": parseFloat(item['discount']),
                "displayLineTotal": parseFloat(parseFloat(item['sell value']) + parseFloat(item['vat amount'])),
                "fulfilmentType": "delivery"
            })
            orderJSON.payments[0].amount += parseFloat(parseFloat(item['sell value']) + parseFloat(item['vat amount']))
        }

        try{
            await common.requester('post', `https://api.stok.ly/v2/saleorders`, orderJSON, 0)
        } catch (e) {
            console.log(order)
            console.log(e)
            // await common.askQuestion('PAUSE')
        }
        

    }



}

async function run(){
    await getAllOrders()
    await getAllCustomers()
    await getCustomersSheet()
    await getAllItems()
    // await getOrdersSheet()
    await getOrdersSheet2()

    await processOrders()
}

module.exports = {
    run
}
