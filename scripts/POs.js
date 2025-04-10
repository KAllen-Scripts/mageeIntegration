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
async function safeRequest(method, url, data = undefined, maxRetries = 5, retryDelay = 1000) {
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
    await common.loopThrough('Getting Suppliers', 'https://api.stok.ly/v1/suppliers', 'size=1000', '[status]!={1}', (supplier) => {
        try {
            suppliers[parseInt(supplier.accountReference)] = supplier.supplierId;
        } catch (error) {}
    });
}

// Get all locations from API
async function getAllLocations() {
    await common.loopThrough('Getting Locations', 'https://api.stok.ly/v0/locations', 'size=1000', '[status]=={0}', async (location) => {
        locations[location.name.toLowerCase().trim()] = location.locationId;
        try{locationLookup[location.name.split('-')[1].trim()] = location.locationId}catch{}
    });
}

// Get all items from API with enhanced data storage
async function getAllItems() {
    await common.loopThrough('Getting Items', 'https://api.stok.ly/v0/items', 'size=1000&sortDirection=ASC&sortField=name', '([status]!={1})', (item) => {
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
                
                // Track transfers by supplier, warehouse, and date
                const supplierCode = row['supplier code'];
                const warehouseName = row['warehouse name'];
                const receivedQuantity = parseFloatSafe(row['order received quantity'], 2) || 0;

                // Only process rows with valid data for transfers and non-zero received quantity
                if (supplierCode && warehouseName && deliveryDate && receivedQuantity > 0) {
                    // Initialize supplier if it doesn't exist
                    if (!allTransfers[supplierCode]) {
                        allTransfers[supplierCode] = {};
                    }
                    
                    // Initialize warehouse if it doesn't exist
                    if (!allTransfers[supplierCode][warehouseName]) {
                        allTransfers[supplierCode][warehouseName] = {};
                    }
                    
                    // Initialize date if it doesn't exist
                    if (!allTransfers[supplierCode][warehouseName][deliveryDate]) {
                        allTransfers[supplierCode][warehouseName][deliveryDate] = {
                            items: {}
                        };
                    }
                    
                    // Add or update item quantity - skip zero quantities
                    if (receivedQuantity > 0) {
                        if (!allTransfers[supplierCode][warehouseName][deliveryDate].items[skuCode]) {
                            allTransfers[supplierCode][warehouseName][deliveryDate].items[skuCode] = receivedQuantity;
                        } else {
                            allTransfers[supplierCode][warehouseName][deliveryDate].items[skuCode] += receivedQuantity;
                        }
                    }
                }
                
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
    return null
}

/**
 * Process orders by creating purchase orders for each sale order
 */
async function makeOrders() {
    for (const orderNumber of Object.keys(saleOrders)) {

       try{
        const order = saleOrders[orderNumber];
        console.log(`Processing order ${orderNumber}...`);

        let locationId = locationLookup[order.supplier]

        if (locationId == undefined){
            await common.requester('post', `https://api.stok.ly/v0/locations`, {
                "name": `${order.supplierName} - ${order.supplier}`,
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
            })
        }

        let locationContactId = await common.requester('get', `https://api.stok.ly/v0/locations/${locationId}/contacts`).then(r=>{return r?.data?.data?.[0]?.contactId})

        if(locationContactId == undefined){
            await common.requester('patch', `https://api.stok.ly/v0/locations/${locationId}`, {
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
            locationContactId = await common.requester('get', `https://api.stok.ly/v0/locations/${locationId}/contacts`).then(r=>{return r?.data?.data?.[0]?.contactId})
        }
        
        // Create or update supplier
        const supplierMethod = suppliers[order.supplier] ? 'patch' : 'post';
        const supplierEndpoint = `https://api.stok.ly/v1/suppliers${supplierMethod === 'patch' ? '/' + suppliers[order.supplier] : ''}`;
        
        const customerData = customersFromSheet[order.supplier] || {};
        const supplierName = customerData.name || order.supplierName || `Supplier ${order.supplier}`;
        const countryCode = customerData['country code'] || order.countryCode || 'GB';

        let supplierContactId;
        let supplierId = suppliers[order.supplier];

        // Get supplier contact if supplier exists
        if (supplierId) {
            const contactResponse = await common.requester('get', `https://api.stok.ly/v1/suppliers/${supplierId}/contacts`);
            supplierContactId = contactResponse.data.data[0]?.contactId;
        }

        if (!supplierId || supplierContactId === undefined) {
            // Create supplier payload
            const supplierPayload = {
                "name": supplierName,
                "type": 1,
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
            const supplierResponse = await common.requester(supplierMethod, supplierEndpoint, supplierPayload);
            await common.sleep(200);
            supplierId = supplierResponse.data.data.id;
            
            // Now get the supplier contact
            const contactResponse = await common.requester('get', `https://api.stok.ly/v1/suppliers/${supplierId}/contacts`);
            supplierContactId = contactResponse.data.data[0]?.contactId;
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
            const skuCode = (item['sku code'] || '').toLowerCase().trim();
            
            // Create or update item if it doesn't exist
            if (!items[skuCode]) {
                const itemPayload = {
                    "isSold": true,
                    "acquisition": 0,
                    "format": 0,
                    "name": item['sku code'],
                    "sku": item['rm name'],
                    "unitsOfMeasure": [
                        {
                            "supplierId": supplierId,
                            "supplierSku": item['rm name'],
                            "cost": {
                                "amount": parseFloatSafe(item['order price'], 2) || 0,
                                "currency": order.currency
                            },
                            "currency": order.currency,
                            "quantityInUnit": 1
                        }
                    ]
                };
                
                console.log(`Creating new item: ${skuCode}`);
                const itemResponse = await common.requester('post', 'https://api.stok.ly/v0/items', itemPayload);
                
                // Save item information
                items[skuCode] = {
                    itemId: itemResponse.data.data.id,
                    name: item['sku code'],
                    sku: item['sku code']
                };
                
                // Wait a moment for the API to process the new item
                await common.sleep(200);
            }
            
            // Get unit of measure
            const uomResponse = await common.requester('get', `https://api.stok.ly/v0/items/${items[skuCode].itemId}/units-of-measure`);
            let unitOfMeasure = uomResponse.data.data[0];
            
            // Create unit of measure if needed
            if (!unitOfMeasure) {
                const uomPayload = {unitsOfMeasure:[{
                    "supplierId": supplierId,
                    "supplierSku": item['rm name'] || item['sku code'],
                    "cost": {
                        "amount": parseFloatSafe(item['order price'], 2) || 0,
                        "currency": order.currency
                    },
                    "currency": order.currency,
                    "quantityInUnit": 1
                }]};
                
                console.log(`Creating UOM for item: ${skuCode}`);
                await common.requester('patch', `https://api.stok.ly/v0/items/${items[skuCode].itemId}`, uomPayload);
                
                // Get the UOM again
                const newUomResponse = await common.requester('get', `https://api.stok.ly/v0/items/${items[skuCode].itemId}/units-of-measure`);
                unitOfMeasure = newUomResponse.data.data[0];
            }
            
            // Add item to purchase order with adjusted quantity and price
            const orderQuantity = parseFloatSafe(item['order quantity'], 2);
            const receivedQuantity = parseFloatSafe(item['order received quantity'], 2) || 0;
            const quantityToUse = Math.max(orderQuantity, receivedQuantity);
            
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
                "quantity": quantityToUse,
                "price": price,
                "tax": tax,
                "discount": 0,
                "displayPrice": displayPrice,
                "displayTax": displayTax,
                "displayDiscount": 0,
                "unitOfMeasureId": unitOfMeasure.unitOfMeasureId,
                "supplierQuantityInUnit": unitOfMeasure.quantityInUnit,
                "supplierCost": unitOfMeasure.cost,
                "supplierSku": unitOfMeasure.supplierSku,
                "expectedDelivery": new Date(item['shipment date'].replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3')).toISOString(),
                "unitCost": unitOfMeasure.cost,
                "taxRate": 0
            });
        }

        // Create purchase order
        const poResponse = await common.requester('post', 'https://api.stok.ly/v0/purchase-orders', purchaseOrder);
        const purchaseOrderId = poResponse.data.data.id;
        
        await common.requester('post', `https://api.stok.ly/v0/purchase-orders/${purchaseOrderId}/approvals`, {"forced":false});
        await common.requester('post', `https://api.stok.ly/v0/purchase-orders/${purchaseOrderId}/submissions`, {"forced":false});
        
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
       } catch {

       }
        

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
                        "exchangeRate": delivery.exchangeRate,
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
                    const expectedQuantity = round(remainingQuantities[skuCode], 2);
                    
                    // Reduce the remaining quantity for this SKU
                    remainingQuantities[skuCode] = round(remainingQuantities[skuCode] - (item.quantity || 0), 2);
                    
                    // Ensure we don't go below zero
                    if (remainingQuantities[skuCode] < 0) {
                        remainingQuantities[skuCode] = 0;
                    }
                    
                    receipt.items.push({
                        "itemId": items[skuCode].itemId,
                        "quantityReceived": round(item.quantity || 0, 2),
                        "quantityBlemished": 0,
                        "quantityExpected": expectedQuantity,
                        "totalCost": {
                            "amount": round((item.price || 0) + (item.tax || 0), 2),
                            "currency": delivery.currency
                        }
                    });
        
                    invoice.invoices[0].items.push({
                        "referenceType": "item",
                        "referenceId": items[skuCode].itemId,
                        "quantity": round(item.quantity || 0, 2),
                        "displayPrice": round((item.unitPrice || 0) * (item.quantity || 0), 2),
                        "displayTax": round((((item.unitPrice || 0) * (item.quantity || 0)) * delivery.taxMod) - ((item.unitPrice || 0) * (item.quantity || 0)), 2),
                        "itemName": item.itemName || "Unknown",
                        "itemSku": item.itemSku || "Unknown"
                    });
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
                let goodsReceiptId = await safeRequest('post', 'https://api.stok.ly/v0/goods-receipts', receipt);
                console.log(`Created goods receipt: ${goodsReceiptId.data.data.id}`);
                
                try {
                    await safeRequest('post', `https://api.stok.ly/v0/goods-receipts/${goodsReceiptId.data.data.id}/completions`);
                    console.log(`Completed goods receipt: ${goodsReceiptId.data.data.id}`);
                } catch (error) {
                    console.error(`Failed to complete goods receipt ${goodsReceiptId.data.data.id}: ${error.message}`);
                    // Continue anyway
                }
                
                try {
                    await safeRequest('post', `https://api.stok.ly/v0/purchase-orders/${delivery.purchaseOrderId}/add-invoices`, invoice);
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
            console.error(`Continuing with next delivery...`);
            // Continue with next delivery
        }
    }
}

async function processTransfers(){
    for (const supplier of Object.keys(allTransfers)){
        for (const location of Object.keys(allTransfers[supplier])){

            for (const date of Object.keys(allTransfers[supplier][location])){
                let locationId = await getLocation(location)
                let transfer = {
                    "sourceLocationId": locationLookup[supplier],
                    "destinationLocationId": locationId,
                    "courierRequired": false,
                    "items": []
                }

                for (const item of Object.keys(allTransfers[supplier][location][date].items)){
                    console.log(item)
                    transfer.items.push({
                        "itemId": items[item.toLowerCase().trim()].itemId,
                        "quantity": allTransfers[supplier][location][date].items[item]
                    })
                }

                try{
                    await common.requester('post', 'https://api.stok.ly/v0/stock-transfers', transfer).then(async r=>{
                        await common.requester('post', `https://api.stok.ly/v0/stock-transfers/${r.data.data.id}/submissions`)
                    })
                }catch (e) {
                    console.log(e)
                }
            }
        }
    }
}

// Main function
async function run() {
    // try {
        // try {
        //     await getAllSuppliers();
        // } catch (error) {
        //     console.error(`Error getting suppliers: ${error.message}`);
        //     // Continue anyway, some suppliers might work
        // }
        
        try {
            await getAllItems();
        } catch (error) {
            console.error(`Error getting items: ${error.message}`);
            // Continue anyway, some items might work
        }
        
        // try {
        //     await getCustomersSheet();
        // } catch (error) {
        //     console.error(`Error getting customer sheet: ${error.message}`);
        //     // Continue anyway with empty customer data
        // }
        
        try {
            await getOrdersSheet();
        } catch (error) {
            console.error(`Error getting orders sheet: ${error.message}`);
            // This is more critical, but continue with whatever orders we have
        }
        
        await getAllLocations();
        
        // await makeOrders();
        
        // // Sort the global deliveries by date (oldest to newest)
        // allDeliveries.sort((a, b) => {
        //     // Convert string dates to numbers for comparison
        //     const dateA = parseInt(a.date);
        //     const dateB = parseInt(b.date);
        //     return dateA - dateB;
        // });
        
        // Log the global deliveries tracker
        console.log(`Total delivery events tracked: ${allDeliveries.length}`);
        
        // await processDeliveries();

        await processTransfers()
        
        console.log('Import completed successfully');
    // } catch (error) {
    //     console.error(`Import process encountered errors but continued: ${error.message}`);
    //     console.log('Import completed with some errors');
    // }
}

module.exports = {
    run
};


let i = {
    '1134': {
        Paveco: {
            20230613:{
                items:{
                    'BLZ-BLU8-24': 24
                }
            }
        }
    }
}