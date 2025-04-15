let common = require('../common.js');
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);


// Input files
let customerSheet = '../files/manufacturing/suppliers.csv';
let ordersSheet = '../files/purchaseOrders/RMPO_20241216155110.csv';

// Data storage
let customersFromSheet = {};
let items = {};
let saleOrders = {};
let suppliers = {};
let locations = {};
let locationLookup = {};
// Global tracker for all deliveries across all orders
let allDeliveries = [];
let allTransfers = {};
let transferArr = [];

let existsingPOs = []

const multiplicationExceptions = ['Cloth', 'Fully Factored']

const altSKUExceptions = ['Button','Lining','Waistband/Pocket','Alcantara','Melton','Woven Label','Ticket/Story','Sundry']

let logWrite = fs.createWriteStream('./PODebug.txt', {flags: 'a'})

// Standard VAT/sales tax rates
let standardRates = {
    "GB": 20,
    "IE": 23,
    "SE": 25,
    "DE": 19,
    "US": {
        "Spring City": 6.6,
        "NEW YORK NY 10028": 8.875,
        "NEW HOPE": 6.6,
        "HUDSON": 6.75,
        "BOYCE": 5.3,
        "MANCHESTER CENTRE": 6,
        "BRONX": 8.875,
        "EUREKA": 8.5,
        "HUNTSVILLE": 9,
        "ANNAPOLIS": 6,
        "KINGSTON": 8,
        "SEATTLE": 10.1,
        "LENOX": 6.25,
        "SPRING LAKE": 6.625,
        "BRUNSWICK": 5.5,
        "WASHINGTON 20007": 6,
        "BETHLEHEM": 6.6,
        "WHITMAN": 6.25,
        "CAMBRIDGE": 6.25,
        "WILLIAMSBURG": 6,
        "PORTSMOUTH": 0,
        "55105 ST PAUL": 7.875,
        "OCCOQUAN": 6,
        "NEWPORT": 7,
        "NILES": 10.25,
        "MILLBROOK": 8.125,
        "TOPSFIELD MA 01983": 6.25,
        "BELLEVUE": 10,
        "CHARLESTON SC 29401": 9,
        "WASHINGTON 98009-0139": 10.1,
        "ACCOKEEK": 6
    },
    "FI": 24,
    "FR": 20,
    "CH": 7.7,
    "DK": 25,
    "XI": 20,
    "IT": 22,
    "BE": 21,
    "JP": 10,
    "NL": 21,
    "RU": 20,
    "CA": 5,
    "JE": 5,
    "AT": 20,
    "CZ": 21,
    "AU": 10,
    "LU": 17,
    "GG": 0,
    "ES": 21,
    "ZA": 15,
    "GR": 24
};

// Helper function for precise rounding
function round(value, decimals = 2) {
    if (typeof value !== 'number' || isNaN(value)) return 0;
    return Number(Math.round(parseFloat(value + 'e' + decimals)) + 'e-' + decimals);
}

// Helper function to parse floats safely with rounding
function parseFloatSafe(value, decimals = 2) {
    if (!value || isNaN(value)) return 0;
    return round(parseFloat(value), decimals);
}

// Helper function to handle request failures with automatic retries
async function safeRequest(method, url, data = undefined, maxRetries = 10, retryDelay = 1000) {
    let retries = 0;
    
    while (retries <= maxRetries) {
        try {
            if (retries > 0) {
                console.log(`Retry attempt ${retries}/${maxRetries} for ${method.toUpperCase()} ${url}...`);
            }
            
            return await common.requester(method, url, data);
        } catch (error) {
            retries++;
            
            console.error(`==== REQUEST FAILED (Attempt ${retries}/${maxRetries}) ====`);
            console.error(`URL: ${url}`);
            console.error(`Method: ${method}`);
            
            if (data) {
                console.error("Request payload:");
                console.error(JSON.stringify(data));
            }
            
            if (error.response) {
                console.error(`Status: ${error.response.status}`);
                console.error("Response data:");
                console.error(JSON.stringify(error.response.data));
            } else {
                console.error(`Error: ${error.message}`);
            }
            
            // If we've used all our retry attempts, throw the error instead of exiting
            if (retries > maxRetries) {
                console.error(`All ${maxRetries} retry attempts failed.`);
                throw new Error(`Request to ${url} failed after ${maxRetries} attempts: ${error.message}`);
            }
            
            // Wait before retrying (with exponential backoff)
            const backoffTime = retryDelay * Math.pow(2, retries - 1);
            console.log(`Waiting ${backoffTime}ms before retry...`);
            await common.sleep(backoffTime);
        }
    }
}

// Get all suppliers from API
async function getAllSuppliers() {
    await common.loopThrough('Getting Suppliers', `https://${global.enviroment}/v1/suppliers`, 'size=1000', '[status]!={1}', (supplier) => {
        try {
            suppliers[parseInt(supplier.accountReference)] = supplier.supplierId;
        } catch (error) {}
    });
}

async function getAllPOs() {
    await common.loopThrough('Getting POs', `https://${global.enviroment}/v0/purchase-orders`, 'size=1000', '', (PO) => {
        try {
            existsingPOs.push(PO.externalReference.split('||')[1].trim())
        } catch (error) {}
    });
}

// Get all locations from API
async function getAllLocations() {
    await common.loopThrough('Getting Locations', `https://${global.enviroment}/v0/locations`, 'size=1000', '[status]=={0}', async (location) => {
        locations[location.name.toLowerCase().trim()] = location.locationId;
        try{locationLookup[location.name.split('-')[1].trim()] = location.locationId}catch{}
    });
}

// Get all items from API with enhanced data storage
async function getAllItems() {
    await common.loopThrough('Getting Items', `https://${global.enviroment}/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '([status]!={1})', (item) => {
        // Store item with more complete information
        items[item.name.toLowerCase().trim()] = {
            itemId: item.itemId,
            name: item.name,
            sku: item.sku
        };
    });
}

// Read customer data from CSV
async function getCustomersSheet() {
    return new Promise((resolve, reject) => {
        fs.createReadStream(customerSheet)
            .pipe(csv.parse({ headers: headers => headers.map(h => h.toLowerCase().trim()) }))
            .on('error', error => {
                console.error(`Error reading customer sheet: ${error.message}`);
                process.exit(1);
            })
            .on('data', row => {
                let customerCode = isNaN(row['code']) ? row['code'] : parseInt(row['code']);
                customersFromSheet[customerCode] = row;
            })
            .on('end', () => {
                resolve();
            });
    });
}

// Read orders data from CSV
async function getOrdersSheet() {
    return new Promise((resolve, reject) => {
        // Temporary storage for items by SKU before merging
        const tempItems = {};
        
        // Track deliveries by date for closed items
        const deliveryDateItems = {};
        
        fs.createReadStream(ordersSheet)
            .pipe(csv.parse({ headers: headers => headers.map(h => h.toLowerCase().trim()) }))
            .on('error', error => {
                console.error(`Error reading orders sheet: ${error.message}`);
                process.exit(1);
            })
            .on('data', row => {
                // Skip if the item is cancelled
                if (row['colour order status']?.toLowerCase()?.trim() === 'cancelled') {
                    return;
                }
                
                const orderNumber = row['order number'];
                const deliveryDate = row['delivery date'];
                const skuCode = (row['sku code'] || '').toLowerCase().trim();
                const isClosed = row['colour order status']?.toLowerCase()?.trim() === 'closed';

                // Initialize order if it doesn't exist
                if (!saleOrders[orderNumber]) {
                    saleOrders[orderNumber] = {
                        supplier: parseInt(row['supplier code']),
                        status: row['order status'],
                        supplierName: row['supplier name'],
                        manufacturer: row['warehouse name'],
                        countryCode: row['country code'],
                        date: new Date(row['order create date'].replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3')).toISOString(),
                        items: [],
                        type: row['order type name'],
                        number: orderNumber,
                        ref: row['order reference'],
                        currency: row['order currency'],
                        exampleBaseCurrency: parseFloatSafe(row['order value in base currency'], 6) || 0,
                        exampleOrderCurrency: parseFloatSafe(row['order value in order currency'], 6) || 0,
                        deliveries: {} // Track deliveries by date
                    };
                    
                    // Initialize tempItems for this order
                    tempItems[orderNumber] = {};
                    
                    // Initialize deliveryDateItems for this order
                    deliveryDateItems[orderNumber] = {};
                }
                
                // Check if the item exists in the system
                const missing = !items[skuCode];
                
                // If the item is closed and has a delivery date, track it
                if (isClosed && deliveryDate) {
                    if (!deliveryDateItems[orderNumber][deliveryDate]) {
                        deliveryDateItems[orderNumber][deliveryDate] = [];
                    }
                    
                    // Add this delivery record
                    deliveryDateItems[orderNumber][deliveryDate].push({
                        skuCode,
                        rmName: row['rm name'],
                        quantity: parseFloatSafe(row['order received quantity'], 2) || 0,
                        itemDetails: { ...row }
                    });
                }
                
                // Use SKU code as a key to merge items
                if (!tempItems[orderNumber][skuCode]) {
                    tempItems[orderNumber][skuCode] = { 
                        ...row, 
                        missing,
                        'order quantity': parseFloatSafe(row['order quantity'], 2) || 0,
                        'order value in order currency': parseFloatSafe(row['order value in order currency'], 2) || 0,
                        'order value in base currency': parseFloatSafe(row['order value in base currency'], 2) || 0,
                        'order received quantity': parseFloatSafe(row['order received quantity'], 2) || 0
                    };
                } else {
                    // Merge by adding quantities and values with careful rounding
                    tempItems[orderNumber][skuCode]['order quantity'] = round(
                        tempItems[orderNumber][skuCode]['order quantity'] + parseFloatSafe(row['order quantity'], 2), 2
                    );
                    tempItems[orderNumber][skuCode]['order value in order currency'] = round(
                        tempItems[orderNumber][skuCode]['order value in order currency'] + parseFloatSafe(row['order value in order currency'], 2), 2
                    );
                    tempItems[orderNumber][skuCode]['order value in base currency'] = round(
                        tempItems[orderNumber][skuCode]['order value in base currency'] + parseFloatSafe(row['order value in base currency'], 2), 2
                    );
                    tempItems[orderNumber][skuCode]['order received quantity'] = round(
                        tempItems[orderNumber][skuCode]['order received quantity'] + parseFloatSafe(row['order received quantity'], 2), 2
                    );
                }
            })
            .on('end', () => {
                // Transfer merged items to the saleOrders
                for (const orderNumber in tempItems) {
                    for (const skuCode in tempItems[orderNumber]) {
                        saleOrders[orderNumber].items.push(tempItems[orderNumber][skuCode]);
                    }
                    
                    // Transfer delivery date information
                    saleOrders[orderNumber].deliveries = deliveryDateItems[orderNumber];
                }
                resolve();
            });
    });
}


async function getLocation(manufacturerName) {
    for (const location in locations){
        if (location.toLowerCase().trim().includes(manufacturerName.toLowerCase().trim())){return locations[location]}
    }
    console.log('///////////////////////////////////////////////////////////////////////////////////\n\nNUMBER 2 \n\n///////////////////////////////////////////////////////////////////////////////////')
    return safeRequest('post', `https://${global.enviroment}/v0/locations`, {
        "name": manufacturerName,
        "address": {
            "line1": "Default",
            "line2": "",
            "city": "Default",
            "country": "IE",
            "postcode": "XXX XXX",
            "region": ""
        },
        "contacts": []
    }).then(r=>{
        locations[manufacturerName.toLowerCase().trim()] = r.data.data.id
        return r.data.data.id
    })
}


async function makeOrders(orderNumberDebug = false) {
    for (const orderNumber of Object.keys(saleOrders)) {
        try{
            if(existsingPOs.includes(orderNumber)){continue}
            if (orderNumberDebug != false && orderNumber != orderNumberDebug){continue}
            const order = saleOrders[orderNumber];
            console.log(`Processing order ${orderNumber}...`);
    
            let locationId = locationLookup[order.supplier]
            let locationContactId
    
            if (locationId == undefined){
    
                let locationName = `${order.supplierName} - ${order.supplier}`
    
                console.log('///////////////////////////////////////////////////////////////////////////////////\n\nNUMBER 1 \n\n///////////////////////////////////////////////////////////////////////////////////')
    
                await common.requester('post', `https://${global.enviroment}/v0/locations`, {
                    "name": locationName,
                    "address": {
                        "line1": "default",
                        "city": "default",
                        "country": "IE",
                        "postcode": "XXX XXX",
                        "region": ""
                    },
                    "contacts": [
                        {
                            "forename": "contact",
                            "surname": "",
                            "email": "contact@contact.com",
                            "tags": [
                                "Delivery Contact"
                            ],
                            "name": {
                                "forename": "contact",
                                "surname": ""
                            }
                        }
                    ]
                }).then(async r=>{
                    locationLookup[order.supplier] = r.data.data.id
                    locationId = r.data.data.id
                    locations[locationName.toLowerCase().trim()] = r.data.data.id
    
                    let attempts = 0;
    
                    while (locationContactId === undefined && attempts < 100) {
                        const contactResponse = await safeRequest('get', `https://${global.enviroment}/v0/locations/${locationId}/contacts`);
                        locationContactId = contactResponse?.data?.data?.[0]?.contactId;
                        
                        if (locationContactId === undefined) {
                            attempts++;
                            await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 1 second before retry
                        }
                    }
                })
            }
    
            let attempts = 0;
            while (locationContactId === undefined && attempts < 1000) {
                const contactResponse = await safeRequest('get', `https://${global.enviroment}/v0/locations/${locationId}/contacts`);
                locationContactId = contactResponse?.data?.data?.[0]?.contactId;
                
                if (locationContactId === undefined) {
                    attempts++;
                    await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 1 second before retry
                }
            }
    
            if(locationContactId == undefined){
                await safeRequest('patch', `https://${global.enviroment}/v0/locations/${locationId}`, {
                    "contacts": [
                        {
                            "forename": "contact",
                            "surname": "",
                            "email": "contact@contact.com",
                            "tags": [
                                "Delivery Contact"
                            ],
                            "name": {
                                "forename": "contact",
                                "surname": ""
                            }
                        }
                    ]
                });
                let attempts = 0;
                while (locationContactId === undefined && attempts < 1000) {
                    const contactResponse = await safeRequest('get', `https://${global.enviroment}/v0/locations/${locationId}/contacts`);
                    locationContactId = contactResponse?.data?.data?.[0]?.contactId;
                    
                    if (locationContactId === undefined) {
                        attempts++;
                        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 1 second before retry
                    }
                }
            }
    
    
            // Create or update supplier
            const supplierMethod = suppliers[order.supplier] ? 'patch' : 'post';
            const supplierEndpoint = `https://${global.enviroment}/v1/suppliers${supplierMethod === 'patch' ? '/' + suppliers[order.supplier] : ''}`;
            
            const customerData = customersFromSheet[order.supplier] || {};
            const supplierName = customerData.name || order.supplierName || `Supplier ${order.supplier}`;
            const countryCode = customerData['country code'] || order.countryCode || 'GB';
    
            let supplierContactId;
            let supplierId = suppliers[order.supplier];
    
            // Get supplier contact if supplier exists
            if (supplierId) {
                const contactResponse = await safeRequest('get', `https://${global.enviroment}/v1/suppliers/${supplierId}/contacts`);
                supplierContactId = contactResponse.data.data[0]?.contactId;
            }
    
            if (!supplierId || supplierContactId == undefined) {
                // Create supplier payload
                const supplierPayload = {
                    "name": supplierName,
                    "type": 1,
                    dropShips: false,
                    "accountReference": order.supplier,
                    "vatNumber": {
                        "value": customerData['vat registration number'] || String(order.supplier),
                        "country": countryCode
                    },
                    "address": {
                        "line1": customerData['address line1'] || 'undefined',
                        "line2": customerData['address line2'] || '',
                        "city": customerData['city'] || 'undefined',
                        "region": customerData['county'] || '',
                        "country": countryCode,
                        "postcode": "undefined"
                    },
                    "currency": customerData.currency || order.currency,
                    "contacts": [
                        {
                            "forename": supplierName,
                            "surname": "",
                            "email": customerData['contact email'] || 'default@default.com',
                            "phone": customerData['contact telephone'] || '121212121212',
                            "tags": ["Delivery Contact"],
                            "name": {
                                "forename": supplierName,
                                "surname": ""
                            }
                        }
                    ]
                };
                
                // Create or update supplier and get ID
                const supplierResponse = await safeRequest(supplierMethod, supplierEndpoint, supplierPayload);
                await common.sleep(200);
                supplierId = supplierResponse.data.data.id;
                suppliers[order.supplier] = supplierResponse.data.data.id;
                
                let attempts = 0;
                
                while (supplierContactId === undefined && attempts < 5) {
                  const contactResponse = await safeRequest('get', `https://${global.enviroment}/v1/suppliers/${supplierId}/contacts`);
                  supplierContactId = contactResponse.data.data[0]?.contactId;
                  
                  if (supplierContactId === undefined) {
                    attempts++;
                    await new Promise(resolve => setTimeout(resolve, 2000));
                  }
                }
            }
    
            // Create purchase order object
            let exchangeRate;
            if (order.exampleBaseCurrency && order.exampleOrderCurrency && order.exampleOrderCurrency !== 0) {
                // Calculate exchange rate with proper rounding to 6 decimal places
                exchangeRate = round(order.exampleOrderCurrency / order.exampleBaseCurrency, 6).toFixed(6);
            } else {
                exchangeRate = "1.000000";
            }
    
            const exchangeRateFloat = parseFloat(exchangeRate);
                
            const purchaseOrder = {
                "currency": order.currency,
                "items": [],
                "location": {
                    "locationContactId": locationContactId,
                    "locationId": locationId
                },
                "supplier": {
                    "supplierContactId": supplierContactId,
                    "supplierId": supplierId
                },
                "additionalCosts": undefined,
                "useSystemExchangeRate": false,
                "exchangeRate": exchangeRateFloat,
                externalReference: order.ref + " || " + orderNumber
            };
            
            // Calculate tax modifier
            let taxMod = 1.23; // Default 23% tax
            const country = customerData['country code'] || order.countryCode || 'GB';
            
            if (country === 'US') {
                const city = customerData['city'] || '';
                if (standardRates.US[city]) {
                    taxMod = 1 + (standardRates.US[city] / 100);
                } else {
                    taxMod = 1.07; // Default US tax
                }
            } else if (standardRates[country]) {
                taxMod = 1 + (standardRates[country] / 100);
            }
            
            // Process items for purchase order
            for (const item of order.items) {
                const skuCode = (!altSKUExceptions.includes(item['rm type name']) ? item['sku code'] : item['rm name']).toLowerCase().trim()
                const skuCodeCasePreserved = (!altSKUExceptions.includes(item['rm type name']) ? item['sku code'] : item['rm name'])
                
                // Create or update item if it doesn't exist
                if (!items[skuCode]) {
                    const itemPayload = {
                        "isSold": true,
                        "acquisition": 0,
                        "format": 0,
                        "name": item['sku code'],
                        "sku": skuCodeCasePreserved,
                        "unitsOfMeasure": [
                            {
                                "supplierId": supplierId,
                                "supplierSku": skuCodeCasePreserved,
                                "cost": {
                                    "amount": !multiplicationExceptions.includes(item['rm type name']) ? parseFloatSafe(item['order price'], 2) * 100 || 0 : (parseFloatSafe(item['order price'], 2) || 0),
                                    "currency": order.currency
                                },
                                "currency": order.currency,
                                "quantityInUnit": !multiplicationExceptions.includes(item['rm type name']) ? 100 : 1
                            }
                        ]
                    };
                    
                    console.log(`Creating new item: ${skuCode}`);
                    const itemResponse = await safeRequest('post', `https://${global.enviroment}/v0/items`, itemPayload);
                    
                    // Save item information
                    items[skuCode] = {
                        itemId: itemResponse.data.data.id,
                        name: item['sku code'],
                        sku: skuCode
                    };
                
                }
                
                // Get unit of measure
                let unitOfMeasure;
                let attempts = 0;
    
                while (unitOfMeasure === undefined && attempts < 1000) {
                    const uomResponse = await safeRequest('get', `https://${global.enviroment}/v0/items/${items[skuCode].itemId}/units-of-measure`);
                    unitOfMeasure = uomResponse.data.data[0];
                    
                    if (unitOfMeasure === undefined) {
                        attempts++;
                        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 1 second before retry
                    }
                }
                
                // Create unit of measure if needed
                if (!unitOfMeasure) {
                    const uomPayload = {unitsOfMeasure:[{
                        "supplierId": supplierId,
                        "supplierSku": skuCode,
                        "cost": {
                            "amount": !multiplicationExceptions.includes(item['rm type name']) ? parseFloatSafe(item['order price'], 2) * 100 || 0 : (parseFloatSafe(item['order price'], 2) || 0),
                            "currency": order.currency
                        },
                        "currency": order.currency,
                        "quantityInUnit": !multiplicationExceptions.includes(item['rm type name']) ? 100 : 1
                    }]};
                    
                    console.log(`Creating UOM for item: ${skuCode}`);
                    await safeRequest('patch', `https://${global.enviroment}/v0/items/${items[skuCode].itemId}`, uomPayload);
                    
                    // Get the UOM again
                    let unitOfMeasure;
                    let attempts = 0;
        
                    while (unitOfMeasure === undefined && attempts < 1000) {
                        const uomResponse = await safeRequest('get', `https://${global.enviroment}/v0/items/${items[skuCode].itemId}/units-of-measure`);
                        unitOfMeasure = uomResponse.data.data[0];
                        
                        if (unitOfMeasure === undefined) {
                            attempts++;
                            await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 1 second before retry
                        }
                    }
                }
                
                // Add item to purchase order with adjusted quantity and price
                const orderQuantity = parseFloatSafe(item['order quantity'], 2);
                const receivedQuantity = parseFloatSafe(item['order received quantity'], 2) || 0;
                const quantityToUse = round(Math.max(orderQuantity, receivedQuantity), 4);
                
                // Use the order price directly from the CSV and multiply by the appropriate quantity
                const unitPrice = parseFloatSafe(item['order price'], 6) || 0;
                const displayPrice = round((unitPrice) * quantityToUse, 2);
                const price = round((unitPrice * exchangeRateFloat) * quantityToUse, 2);
                
                // Calculate tax amounts based on the adjusted prices
                const tax = round((price * taxMod) - price, 2);
                const displayTax = round((displayPrice * taxMod) - displayPrice, 2);
                
                // Add item to purchase order
                purchaseOrder.items.push({
                    "itemId": items[skuCode].itemId,
                    "lineDiscount": 0,
                    "quantity": round(!multiplicationExceptions.includes(item['rm type name']) ? quantityToUse / 100 : quantityToUse, 4),
                    "price": round(price, 4),
                    "tax": round(tax, 4),
                    "discount": 0,
                    "displayPrice": round(displayPrice, 4), 
                    "displayTax": round(displayTax, 4),
                    "displayDiscount": 0,
                    "unitOfMeasureId": unitOfMeasure.unitOfMeasureId,
                    "supplierQuantityInUnit": round(unitOfMeasure.quantityInUnit, 4),
                    "supplierCost": {
                        "amount": round(unitOfMeasure.cost.amount, 4),
                        "currency": unitOfMeasure.cost.currency
                    },
                    "supplierSku": unitOfMeasure.supplierSku,
                    "expectedDelivery": new Date(item['shipment date'].replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3')).toISOString(),
                    "unitCost": {
                        "amount": round(unitOfMeasure.cost.amount, 4),
                        "currency": unitOfMeasure.cost.currency
                    },
                    "taxRate": 0
                });
                if (allTransfers[orderNumber] == undefined){allTransfers[orderNumber] = {source: `${item['supplier name']} - ${item['supplier code']}`, destinations: {}}}
                if (allTransfers[orderNumber].destinations[item[`warehouse name`]] == undefined){allTransfers[orderNumber].destinations[item[`warehouse name`]] = {}}
                if (allTransfers[orderNumber].destinations[item[`warehouse name`]][item['delivery date']] == undefined){allTransfers[orderNumber].destinations[item[`warehouse name`]][item['delivery date']] = {}}
                allTransfers[orderNumber].destinations[item[`warehouse name`]][item['delivery date']][items[(!altSKUExceptions.includes(item['rm type name']) ? item['sku code'] : item['rm name']).toLowerCase().trim()].itemId] = item['order received quantity']
            }
    
    
            logWrite.write(JSON.stringify(purchaseOrder) + '\r\n\r\n')
    
            // Create purchase order
            const poResponse = await safeRequest('post', `https://${global.enviroment}/v0/purchase-orders`, purchaseOrder);
            const purchaseOrderId = poResponse.data.data.id;
            
            await safeRequest('post', `https://${global.enviroment}/v0/purchase-orders/${purchaseOrderId}/approvals`, {"forced":false});
            await safeRequest('post', `https://${global.enviroment}/v0/purchase-orders/${purchaseOrderId}/submissions`, {"forced":false});
            
            console.log(`Created purchase order for ${orderNumber}, ID: ${purchaseOrderId}`);
            
            // Add the purchase order ID to each delivery date
            if (order.deliveries) {
                for (const deliveryDate in order.deliveries) {
                    if (order.deliveries[deliveryDate]) {
                        order.deliveries[deliveryDate] = {
                            purchaseOrderId: purchaseOrderId,
                            items: order.deliveries[deliveryDate]
                        };
                        
                        // Calculate total cost for each item in this delivery
                        const deliveryItems = order.deliveries[deliveryDate].items.map(item => {
                            // Find the corresponding item in the order to get complete cost data
                            const originalItem = order.items.find(orderItem => 
                                orderItem['sku code'].toLowerCase().trim() === item.skuCode
                            );
                            
                            // Get the total ordered quantity for this item in the entire PO
                            const totalPOQuantity = originalItem ? parseFloatSafe(originalItem['order quantity'], 2) || 0 : 0;
                            
                            // Use the unit price directly from the CSV
                            const unitPrice = originalItem ? parseFloatSafe(originalItem['order price'], 6) || 0 : 0;
                            
                            // For this delivery, use actual received quantity
                            const receivedQuantity = round(item.quantity, 2);
                            
                            // Calculate prices based on the received quantity with proper rounding
                            const price = round(unitPrice * receivedQuantity, 2);
                            const tax = round((price * taxMod) - price, 2);
                            
                            // Calculate display prices for this quantity (in order currency)
                            const displayLinePrice = round(price * parseFloat(exchangeRate), 2);
                            const displayLineTax = round((displayLinePrice * taxMod) - displayLinePrice, 2);
                            
                            return {
                                ...item,
                                totalPOQuantity,
                                unitPrice,
                                price,
                                tax,
                                displayLinePrice,
                                displayLineTax,
                                type: originalItem['rm type name'],
                                // Add additional item information
                                itemName: items[item.skuCode] ? items[item.skuCode].name : item.rmName,
                                itemSku: items[item.skuCode] ? items[item.skuCode].sku : item.skuCode
                            };
                        });
                        
                        // Calculate grand total quantity and cost
                        const totalQuantity = round(deliveryItems.reduce((sum, item) => sum + (item.quantity || 0), 0), 2);
                        const totalDeliveryCost = round(deliveryItems.reduce((sum, item) => sum + (item.price || 0), 0), 2);
                        
                        // Add to global tracker with all needed information
                        allDeliveries.push({
                            date: deliveryDate,
                            purchaseOrderId: purchaseOrderId,
                            currency: order.currency,
                            exchangeRate: exchangeRateFloat,
                            locationId: locationId,
                            supplierId: supplierId,
                            supplierName: supplierName,
                            manufacturer: order.manufacturer,
                            totalQuantity: totalQuantity,
                            totalCost: totalDeliveryCost,
                            items: deliveryItems,
                            taxMod
                        });
                    }
                }
            }
        } catch {}
    }
}

async function processDeliveries() {
    // Track remaining quantities for each SKU
    const remainingQuantities = {};
    
    for (const delivery of allDeliveries) {
        try {
            console.log(`Processing delivery for PO ${delivery.purchaseOrderId} on date ${delivery.date}...`);
            
            let receipt = {
                "locationId": delivery.locationId,
                "items": [],
                "supplierId": delivery.supplierId,
                "referenceType": 0,
                "referenceIds": [
                    delivery.purchaseOrderId
                ]
            };
    
            let invoice = {
                "invoiceApplicationMethod": 0,
                "invoices": [
                    {
                        "invoiceNumber": null,
                        "status": "issued",
                        "currency": delivery.currency,
                        "exchangeRate": round(delivery.exchangeRate, 4),
                        "useSystemExchangeRate": false,
                        "items": []
                    }
                ]
            };
    
            // Process each item in the delivery
            for (const item of delivery.items) {
                try {
                    const skuCode = item.skuCode.toLowerCase().trim();
                    
                    // Check if the item exists in our items dictionary
                    if (!items[skuCode] || !items[skuCode].itemId) {
                        console.error(`Item ${skuCode} not found in items dictionary. Skipping.`);
                        continue;
                    }
                    
                    // Initialize remaining quantity for this SKU if it's the first time seeing it
                    if (remainingQuantities[skuCode] === undefined) {
                        // Use max of ordered quantity or received quantity as the expected total
                        remainingQuantities[skuCode] = Math.max(item.totalPOQuantity || 0, item.quantity || 0);
                    }
                    
                    // Use the current remaining quantity as the expected quantity
                    const expectedQuantity = round(remainingQuantities[skuCode], 4);
                    
                    // Reduce the remaining quantity for this SKU
                    remainingQuantities[skuCode] = round(remainingQuantities[skuCode] - (item.quantity || 0), 4);
                    
                    // Ensure we don't go below zero
                    if (remainingQuantities[skuCode] < 0) {
                        remainingQuantities[skuCode] = 0;
                    }
                    
                    receipt.items.push({
                        "itemId": items[skuCode].itemId,
                        "quantityReceived": round(item.quantity || 0, 4),
                        "quantityBlemished": 0,
                        "quantityExpected": expectedQuantity,
                        "totalCost": {
                            "amount": round((item.price || 0) + (item.tax || 0), 4),
                            "currency": delivery.currency
                        }
                    });
        
                    if (round((item.unitPrice || 0) * (item.quantity || 0), 4) > 0){
                        invoice.invoices[0].items.push({
                            "referenceType": "item",
                            "referenceId": items[skuCode].itemId,
                            "quantity": round(!multiplicationExceptions.includes(item.type) ? round(item.quantity || 0, 4) / 100 : round(item.quantity || 0, 4), 4),
                            "displayPrice": round((item.unitPrice || 0) * (item.quantity || 0), 4),
                            "displayTax": round((((item.unitPrice || 0) * (item.quantity || 0)) * delivery.taxMod) - ((item.unitPrice || 0) * (item.quantity || 0)), 4),
                            "itemName": item.itemName || "Unknown",
                            "itemSku": item.itemSku || "Unknown"
                        });
                    }

                } catch (error) {
                    console.error(`Error processing item ${item.skuCode} in delivery: ${error.message}`);
                    // Continue with next item
                }
            }
            
            // Only proceed if we have items to process
            if (receipt.items.length === 0) {
                console.error(`No valid items to receive for delivery on date ${delivery.date}. Skipping.`);
                continue;
            }
    
            try {
                logWrite.write(JSON.stringify(receipt) + '\r\n\r\n')
                logWrite.write(JSON.stringify(invoice) + '\r\n\r\n')
                
                try {
                    let goodsReceiptId = await safeRequest('post', `https://${global.enviroment}/v0/goods-receipts`, receipt);
                    console.log(`Created goods receipt: ${goodsReceiptId.data.data.id}`);
                    await safeRequest('post', `https://${global.enviroment}/v0/goods-receipts/${goodsReceiptId.data.data.id}/completions`);
                    console.log(`Completed goods receipt: ${goodsReceiptId.data.data.id}`);
                } catch (error) {
                    console.error(`Failed to complete goods receipt ${goodsReceiptId.data.data.id}: ${error.message}`);
                    // Continue anyway
                }
                
                try {
                    await safeRequest('post', `https://${global.enviroment}/v0/purchase-orders/${delivery.purchaseOrderId}/add-invoices`, invoice);
                    console.log(`Added invoice to PO: ${delivery.purchaseOrderId}`);
                } catch (error) {
                    console.error(`Failed to add invoice to PO ${delivery.purchaseOrderId}: ${error.message}`);
                    // Continue anyway
                }
            } catch (error) {
                console.error(`Failed to create goods receipt for delivery on date ${delivery.date}: ${error.message}`);
                // Continue with next delivery
            }
        } catch (error) {
            console.error(`Error processing delivery on date ${delivery.date}: ${error.message}`);
            await common.askQuestion(error)
            console.error(`Continuing with next delivery...`);
            // Continue with next delivery
        }
    }
}

async function processTransfers(){
    for (const transfer of transferArr){
        if (transfer.items.length == 0){continue}
        try{
            await safeRequest('post', `https://${global.enviroment}/v0/stock-transfers`, transfer).then(async r=>{
                await safeRequest('post', `https://${global.enviroment}/v0/stock-transfers/${r.data.data.id}/submissions`)
            })
        } catch {}

    }
}

// Main function
async function run() {

        await getAllPOs()

        try {
            await getAllSuppliers();
        } catch (error) {
            console.error(`Error getting suppliers: ${error.message}`);
            // Continue anyway, some suppliers might work
        }
        
        try {
            await getAllItems();
        } catch (error) {
            console.error(`Error getting items: ${error.message}`);
            // Continue anyway, some items might work
        }
        
        try {
            await getCustomersSheet();
        } catch (error) {
            console.error(`Error getting customer sheet: ${error.message}`);
            // Continue anyway with empty customer data
        }
        
        try {
            await getOrdersSheet();
        } catch (error) {
            console.error(`Error getting orders sheet: ${error.message}`);
        }
        
        await getAllLocations();
        
        await makeOrders();
    
        // Sort the global deliveries by date (oldest to newest)
        allDeliveries.sort((a, b) => {
            // Convert string dates to numbers for comparison
            const dateA = parseInt(a.date);
            const dateB = parseInt(b.date);
            return dateA - dateB;
        });


        for (const orderNumber of Object.keys(allTransfers)){
            for (let destination of Object.keys(allTransfers[orderNumber].destinations)){
                for (const delivery of Object.keys(allTransfers[orderNumber].destinations[destination])){
                    let locationId = await getLocation(destination)
                    let sourceId = await getLocation(allTransfers[orderNumber].source)
                    let transfer = {
                        "sourceLocationId": sourceId,
                        "destinationLocationId": locationId,
                        "courierRequired": false,
                        "items": [],
                        delivery: delivery
                    }
                    for (const item of Object.keys(allTransfers[orderNumber].destinations[destination][delivery])){
                        if (allTransfers[orderNumber].destinations[destination][delivery][item] > 0){
                            transfer.items.push({
                                itemId: item,
                                quantity: allTransfers[orderNumber].destinations[destination][delivery][item]
                            })
                        }
                    }
                    transferArr.push(transfer)
                }  
            }
        }

        transferArr.sort((a, b) => {
            // Convert the date strings to numbers for comparison
            const dateA = parseInt(a.delivery, 10);
            const dateB = parseInt(b.delivery, 10);
            
            // Sort from oldest to newest
            return dateA - dateB;
            });
        
        // Log the global deliveries tracker
        console.log(`Total delivery events tracked: ${allDeliveries.length}`);
        
        await processDeliveries();

        await processTransfers()

        
        console.log('Import completed successfully');
}

module.exports = {
    run
};