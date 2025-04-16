let common = require('../common.js');
const convertCSV = require("json-2-csv");
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);

let customerSheet = '../files/saleOrders/customers.csv';
let ordersSheet = '../files - new/order/CO/CO_20241216155000.csv';
const fmFilePath = '../files - new/FM/250225_FM Product Data - all skus 25.02.25.csv';


let customersFromSheet = {};
let items = {};
let saleOrders = {};
let customerList = {};
let orders = [];
let VATLookup = {};
let ordersToCollect = [];
let nameLookup = {};

const warehouseLocation = '55c142b5-f047-441b-8b06-47df61f94b67';
const sourceChannel = '1d571a7e-47bb-4555-8913-a6c7d846e862';

async function getAllItems(){
    await common.loopThrough('Getting Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]!={2}))%26%26([status]!={1})', async (item)=>{
        items[item.sku.toLowerCase().trim()] = item.itemId
        nameLookup[item.sku.toLowerCase().trim()] = item.name
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

async function processFmCSV() {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(fmFilePath)
            .pipe(csv.parse({ 
                headers: true, 
                trim: true,
                // Handle carriage returns and other whitespace in headers
                transformHeader: (header) => {
                    return header.replace(/\r\n|\n|\r/g, ' ').trim();
                }
            }))
            .on('error', error => {
                console.error('Error parsing FM CSV:', error);
                reject(error);
            })
            .on('data', async row => {
                stream.pause();

                const product = row['SPC'] ? row['SPC'].trim() : '';
                const colorCode = row['Colour'] ? (isNaN(Number(row['Colour'].trim())) ? row['Colour'].trim() : String(Number(row['Colour'].trim()))) : '';
                
                // Skip if essential data is missing
                if (!product || !colorCode) {
                    stream.resume();
                    return;
                }
                
                const productCode = `${product}_${colorCode}`;
                
                VATLookup[productCode.toLowerCase().trim()] = parseFloat(row['Vat Rate']) / 100

                stream.resume();
            })
            .on('end', () => {
                resolve();
            });
    });
}

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
        const stream = fs.createReadStream(ordersSheet)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()

                if (row['product order status'].toLowerCase() != 'cancelled'){

                    let sku = `${row['product code']}_${row['colour code']}-${[row['size code'].trim(), row['fit code'].trim()].join('')}`
                    let parentSku = `${row['product code']}_${row['colour code']}`

                    let missing = false
                    let orderDate = [row['day'].padStart(2, '0'), row['month'].padStart(2, '0'), row['year']].join('-')
                    if (saleOrders[row['order number']] == undefined){
                            
                        saleOrders[row['order number']] = {
                            customer: isNaN(parseInt(row['customer code'])) ? row['customer code'] : parseInt(row['customer code']),
                            date: new Date(orderDate.split('-').reverse().join('-')).toISOString(),
                            items:[],
                            type: row['order type name'],
                            number: row['order number'],
                            ref:  row['order reference'],
                            curency: row['order currency'],
                            exampleBaseCurrency: row['order value in base currency'],
                            exampleOrderCurrency: row['order value in order currency']
                        }
                    }
                    if (items[sku.toLowerCase()] == undefined){
                        missing = true
                    }
                    saleOrders[row['order number']].items.push({...row, missing, sku, parentSku})


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
        try{
            if (customerList[order.customer] == undefined){
                let customerData = {
                    "name": {
                        "forename": customersFromSheet[order.customer].name,
                        "surname": customersFromSheet[order.customer].name
                    },
                    "barcode": order.customer,
                    "billingAddress": {
                        "line1": customersFromSheet[order.customer]['address line1'].length > 2 ? customersFromSheet[order.customer]['address line1'] : 'undefined',
                        "line2": customersFromSheet[order.customer]['address line2'] > 2 ? customersFromSheet[order.customer]['address line2'] : '',
                        "city": customersFromSheet[order.customer]['city'] > 2 ? customersFromSheet[order.customer]['city'] : 'undefined',
                        "region": customersFromSheet[order.customer]['county'] > 2 ? customersFromSheet[order.customer]['county'] : '',
                        "country": customersFromSheet[order.customer]['country code'],
                        "postcode": "undefined"
                      },
                    "type": 1,
                    "contacts": [
                        {
                            "forename": customersFromSheet?.[order.customer]?.['short name'],
                            "surname": "",
                            "email": customersFromSheet?.[order.customer]?.['contact email'],
                            "phone": customersFromSheet?.[order.customer]?.['contact telephone'],
                            "tags": [],
                            "name": customersFromSheet?.[order.customer]?.['short name']
                        }
                    ],
                    "fullName": customersFromSheet?.[order?.customer]?.name
                }
                if (customersFromSheet[order.customer]['contact telephone'].trim() != ''){
                    customerData.phone = customersFromSheet[order.customer]['contact telephone']
                }
                if (customersFromSheet[order.customer]['contact email'].trim() != ''){
                    customerData.email = customersFromSheet[order.customer]['contact email']
                }
                await common.requester('post', `https://api.stok.ly/v0/customers`, customerData).then(r=>{
                    customerList[order.customer] = r.data.data.id
                })
            }
    
            let orderJSON = {
                "stage": "order",
                "createdAt": order.date,
                "sourceType": "magento_two",
                sourceId: sourceChannel,
                "sourceReferenceId": order.number,
                "currency": order.curency,
                "tags": ['protex', 'FB', order.type.replace('FSL Forward Booked Saleline & Limited Edition', 'FSL Forward Booked Saleline & LE')],
                useSystemExchangeRate: false,
                exchangeRate: (order.exampleOrderCurrency / order.exampleBaseCurrency).toFixed(6),
                "items": [],
                payments: [{method: 4, amount: 0}],
                customer:{customerId: customerList[order.customer]},
                shipping: {
                    "name": {
                      "forename": customersFromSheet[order.customer].name.split(' ')[0],
                      "surname": (customersFromSheet[order.customer].name.split(' ').slice(1).join(' ') || 'undefined') == '' ? 'undefined' : customersFromSheet[order.customer].name.split(' ').slice(1).join(' ')
                    },
                    "address": {
                      "line1": customersFromSheet[order.customer]['address line1'],
                      "line2": customersFromSheet[order.customer]['address line2'],
                      "city": customersFromSheet[order.customer]['city'],
                      "region": customersFromSheet[order.customer]['county'],
                      "country": customersFromSheet[order.customer]['country code'],
                      "postcode": "undefined"
                    },
                    option: 'ImportedOrder',
                    cost: 0,
                    tax: 0
                }
            }
    
            const collectArr = []
            for (const item of order.items){
    
                let itemNet = parseFloat(item['order price']) / 1.23
                let itemLineNet = parseFloat(item['order value in order currency']) / 1.23
    
                if (VATLookup[item['parentSku']?.toLowerCase()?.trim()] != undefined){
                    console.log('FOUND')
                    itemNet = parseFloat(item['order price']) / (1 + (parseFloat(VATLookup[item['parentSku'].toLowerCase()])))
                    itemLineNet = parseFloat(item['order value in order currency']) / (1 + (parseFloat(VATLookup[item['parentSku'].toLowerCase()])))
                } else {
                    console.log('FAILED')
                }
        
                if (items[item['sku'].toLowerCase()] == undefined){
                    await common.requester('get', `https://api.stok.ly/v0/items?sortDirection=ASC&sortField=status&filter=(([sku]=={@string;${item['sku']}})))`).then(async r=>{
                        if (r.data.data[0]?.itemId != undefined){
                            items[item['sku'].toLowerCase()] = r.data.data[0]?.itemId
                            return
                        }
                        await common.requester('post', `https://api.stok.ly/v0/items`, {
                            "isSold": true,
                            "acquisition": 0,
                            "format": 0,
                            "name": item['sku'],
                            "sku": item['sku']
                        }).then(r=>{
                            items[item['sku'].toLowerCase()] = r.data.data.id
                            nameLookup[item['sku'].toLowerCase()] = item['sku']
                        })
                    })
                }
                orderJSON.items.push({
                    "type": "item",
                    "referenceId": items[item['sku'].toLowerCase()],
                    "displayPrice": parseFloat(itemNet),
                    "displayTax": parseFloat(parseFloat(item['order price']) - itemNet),
                    "quantity": parseInt(item['order quantity']),
                    "displayLinePrice": parseFloat(itemLineNet),
                    "displayLineTax": parseFloat(parseFloat(item['order value in order currency']) - itemLineNet),
                    "displayLineDiscount": parseFloat(item['order discount value']),
                    "displayLineTotal": parseFloat(item['order value in order currency']),
                    "fulfilmentType": "delivery"
                })
                orderJSON.payments[0].amount += parseFloat(item['order value in order currency'])
    
                if(item['colour order status'].toLowerCase().trim() == 'closed'){
                    collectArr.push({
                        "referenceId": item['sku'].toLowerCase(),
                        "quantity": parseInt(item['order quantity']),
                        "displayPrice": parseFloat(itemNet),
                        "displayTax": parseFloat(parseFloat(item['order price']) - itemNet),
                    })
                }
            }
            try{
                let orderRef = await common.requester('post', `https://api.stok.ly/v2/saleorders`, orderJSON, 0).then(r=>{return r.data.data.id})
                if (collectArr.length > 0){
                    ordersToCollect.push({
                        items: collectArr,
                        locationId: warehouseLocation,
                        orderRef,
                        currency: order.curency,
                        exchangeRate: parseFloat((order.exampleOrderCurrency / order.exampleBaseCurrency).toFixed(6))
                    })
                }
            } catch (err) {
                console.log(err)
            }
        } catch {}
    }
}

async function collectOrders(){

    for (const order of ordersToCollect){
        try{
            let collectObj = {
                locationId: order.locationId,
                items: []
            }
    
            let invoices = {
                "invoices": [
                    {
                        "status": "paid",
                        "currency": order.currency,
                        "exchangeRate": order.exchangeRate,
                        "useSystemExchangeRate": false,
                        "payments": [
                            {
                                "method": 4,
                                "amount": 0
                            }
                        ],
                        "items": []
                    }
                ]
            }
    
    
            let paymentAmount = 0
            
            await common.loopThrough('get', `https://api.stok.ly/v2/saleorders/${order.orderRef}/items`, 'size=1000', '([parentId]=={@null;})', (orderItem)=>{
                for (const item of order.items){
                    if (orderItem.itemSku.toLowerCase().trim() == item.referenceId.toLowerCase().trim()){
                        invoices.invoices[0].items.push({
                            "referenceType": "item",
                            "referenceId": items[item.referenceId.toLowerCase().trim()],
                            "itemName": nameLookup[item.referenceId.toLowerCase().trim()],
                            "quantity": item.quantity,
                            "displayPrice": orderItem.displayPrice,
                            "displayTax": orderItem.displayTax
                        })
                        paymentAmount += (orderItem.displayPrice + orderItem.displayTax) * item.quantity
                        collectObj.items.push({
                            "lineId": orderItem.lineId,
                            "quantity": item.quantity
                        })
                    }
                }
            })
    
            invoices.invoices[0].payments[0].amount = parseFloat(paymentAmount.toFixed(6))
            
            await common.requester('post', `https://api.stok.ly/v2/saleorders/${order.orderRef}/invoices`, invoices)
            await common.requester('post', `https://api.stok.ly/v2/saleorders/${order.orderRef}/collect-items`, collectObj)
        } catch {}
    }
}

async function run(){
    await processFmCSV()
    await getAllOrders()
    await getAllCustomers()
    await getCustomersSheet()
    await getAllItems()
    await getOrdersSheet()

    await processOrders()
    await collectOrders()
}

module.exports = {
    run
}