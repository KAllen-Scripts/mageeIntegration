let common = require('../common.js');
const convertCSV = require("json-2-csv");
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);


let items = {};
let saleOrders = {};
let orders = [];
let barcodeLookup = {};
let nameLookup = {};

let channelDetails = {
	don: {
        channel: 'aade0c0c-77bb-41d0-a9ce-a8a7eec791b6',
        location: 'a74a39fe-9133-4dff-9522-a0c4e2526148',
        sheet: '../files - new/retail orders/don.csv'
    },
    anns: {
        channel: '7895ece6-7fbc-4bce-a826-57aacb0abe78',
        location: '3ef8a574-19f2-4c6d-be2c-d68cb3fc97d9',
        sheet: '../files - new/retail orders/anns.csv'
    },
	web: {
        channel: 'e200bb42-2342-42b7-ab2f-91a5099f99a4',
        sheet: '../files - new/retail orders/web.csv'
    },
	arnotts: {
        channel: '51a8ab9f-0a37-479a-8991-4341075dec0f',
        location: 'aa383293-f340-4fc6-b021-9a3f7633d8a4',
        sheet: '../files - new/retail orders/arnotts.csv'
    },
    // shows: {
    //     channel: '0518c5d1-7d77-4d10-a4e7-c7e851b704ff',
    //     location: 'd9589ab8-f9e1-4b67-b90b-90c619a43fdf',
    //     sheet: '../files - new/retail orders/shows.csv'
    // }
}

const userMap = {
	"4": "a5bd70d5-57ec-4882-9646-b5844f1353c5", // "Breda O",
	"5": "a79f137e-5cf6-42b1-adc3-5c415a9411a4", // "Calum M",
	"11": "ef7e867a-a063-4739-97cb-3fe6ed4262e0", // "Isobel S",
	"14": "839d69b9-673d-4a4f-8389-3886f7435586", // "Kieran C",
	"20": "6122c15a-993c-4c1a-9518-1143e93ab046", // "Ray H",
	"23": "850e1e56-acdb-4764-8f13-f9f9432dbadd", // "Stephen H",
	"24": "7f1afe3e-4457-4820-b457-74c647fd5247", // "Violet W",
	"30": "c6ae56b5-5d31-40de-9750-93a38fb9ccac", // "Marie - Claire B",
	"31": "52f9a864-6d51-48a8-90f4-b0a8392df850", // "Kayna M",
	"33": "6a94f755-becd-477e-a697-23b5a40defbd", // "Graham H",
	"34": "af46c640-cc9a-4799-a4c1-4ede7643a623", // "Martina K",
	"39": "6fde85ab-e57e-4779-8098-149a7ebe577c", // "Ann Marie B",
	"45": "3d52bffe-0491-4f18-bfc9-a49fdb28285f", // "Elle B",
	"47": "4bda404b-3f89-4cc2-a04a-b03dbaf08a5a", // "Dallan M",
	"51": "5725a341-d653-486d-891c-5ea9c60bb682", // "Ava A",
	"59": "624613c9-2e48-4531-b0f1-4088721c1aad", // "Ciara T",
	"63": "5c7b17d4-0672-4270-bc62-389f9e80ccd3", // "Ava M",
	"64": "c66f64d9-52e3-454f-9d74-4ac6bc5f0db6", // "Abbie C",
	"66": "0edf9c20-c4b4-47d6-8aff-e1eabde94ea2", // "Cezanne Mc",
	"69": "88c1d5e5-0308-439d-a7a6-379942e5ff5b" // "Ciara GC"
}

async function getAllItems() {
    await common.loopThrough('Getting Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]!={2}))%26%26([status]!={1})', async (item) => {
        //I know this should be a single object, but the name lookup was added after and I'm not up to refactoring stuff
        items[item.sku.toLowerCase()] = item.itemId
        nameLookup[item.sku.toLowerCase()] = item.name
        try {
            for (const barcode of item.barcodes) {
                barcodeLookup[barcode] = item.itemId
            }
        } catch {}
    })
};

async function getAllOrders() {
    await common.loopThrough('Getting Orders', `https://api.stok.ly/v2/saleorders`, 'sortDirection=ASC&sortField=niceId', '(([sourceId]=*{38ed9f3e-85c6-4d1b-b963-44978c9ba10c}%26%26(([stage]=={order}%26%26[itemStatuses]::{unprocessed}))))%26%26([stage]!={removed})', async (order) => {
        orders.push(order.sourceReferenceId)
    })
};

let rowcount = 1

async function getOrdersSheet(channel) {
    console.log(channel)
    console.log(channelDetails[channel])
    return new Promise((res, rej) => {
        const stream = fs.createReadStream(channelDetails[channel].sheet)
            .pipe(csv.parse({
                headers: headers => headers.map(h => h.replace(/\s+/g, ' ').trim().toLowerCase())
            }))
            .on('error', error => console.error(error))
            .on('data', async row => {
                stream.pause()
                if (row['date'] != '' && !row['date'].includes('Total') && parseInt(row['qty sold']) > 0 && row['type'] != 'GV') {

                    if (saleOrders[row['date'] + channel] == undefined) {
                        saleOrders[row['date'] + channel] = {
                            date: new Date(row['date'].replace(/(\d{2})\/(\d{2})\/(\d{4})/, '$3-$2-$1')).toISOString(),
                            location: channelDetails[channel].location,
                            channel: channelDetails[channel].channel,
                            barcode: row['date'] + channel + 'test4',
							userId: lookupUser(row["operator"]),
                            items: [],
                        }
                    }
                    saleOrders[row['date'] + channel].items.push(row)
                }
                rowcount += 1
                stream.resume()
            })
            .on('end', async () => {
                res()
            })
    })
}


async function processOrders() {
    let orderArr = []
    for (const order in saleOrders) {
        orderArr.push(saleOrders[order])
    }
    orderArr.sort((a, b) => new Date(a.date) - new Date(b.date))
    for (const order of orderArr) {
        let orderJSON = {
            "stage": "order",
            "createdAt": order.date,
            "sourceType": order.location == undefined ? 'magento_two' : 'epos',
            sourceId: order.channel,
            sourceReferenceId: order.location || order.barcode,
            barcode: order.barcode,
            "items": [],
            currency: order.currency ?? 'EUR',
            "invoices": [{
                "status": "paid",
                "currency": order.currency ?? 'EUR',
                "exchangeRate": 1,
                "useSystemExchangeRate": false,
                "payments": [{
                    "method": 4,
                    "amount": 0
                }],
                "items": []
            }],
            invoiced: true,
            invoiceAll: true,
			userId: order.userId
        }




        // Create maps to track items by referenceId
        const itemsMap = {};
        const invoiceItemsMap = {};

        for (const item of order.items) {
            let sku;

            if (items[`${item['spc']}_${item['colour']}-${item['size']}`.toLowerCase().trim()] != undefined) {
                sku = `${item['spc']}_${item['colour']}-${item['size']}`.toLowerCase().trim();
            } else {
                sku = [item['spc'].toLowerCase().trim()];
            }

            if (items[sku] == undefined) {
                continue;
            }

            const referenceId = items[sku];
            const quantity = parseInt(item['qty sold'].replace(/,/g, ''));
            const displayPrice = parseFloat((item['nett value'].replace(/,/g, '') / item['qty sold'].replace(/,/g, '')).toFixed(2));
            const displayTax = parseFloat((item['vat amount'].replace(/,/g, '') / item['qty sold'].replace(/,/g, '')).toFixed(2));
            const displayLinePrice = parseFloat(item['nett value'].replace(/,/g, ''));
            const displayLineTax = parseFloat(item['vat amount'].replace(/,/g, ''));
            const displayLineDiscount = parseFloat(item['discount'].replace(/,/g, ''));
            const displayLineTotal = parseFloat(item['sell value'].replace(/,/g, ''));

            // Check if item with this referenceId already exists in our map
            if (itemsMap[referenceId]) {
                // Update existing item
                itemsMap[referenceId].quantity += quantity;
                itemsMap[referenceId].displayLinePrice += displayLinePrice;
                itemsMap[referenceId].displayLineTax += displayLineTax;
                itemsMap[referenceId].displayLineDiscount += displayLineDiscount;
                itemsMap[referenceId].displayLineTotal += displayLineTotal;

                // Recalculate unit price based on new totals and quantities
                itemsMap[referenceId].displayPrice = parseFloat((itemsMap[referenceId].displayLinePrice / itemsMap[referenceId].quantity).toFixed(2));
                itemsMap[referenceId].displayTax = parseFloat((itemsMap[referenceId].displayLineTax / itemsMap[referenceId].quantity).toFixed(2));
            } else {
                // Create new item
                itemsMap[referenceId] = {
                    "type": "item",
                    "referenceId": referenceId,
                    "displayPrice": displayPrice,
                    "displayTax": displayTax,
                    "quantity": quantity,
                    "displayLinePrice": displayLinePrice,
                    "displayLineTax": displayLineTax,
                    "displayLineDiscount": displayLineDiscount,
                    "displayLineTotal": displayLineTotal,
                    "fulfilmentType": "none"
                };
            }

            // Similarly for invoice items
            if (invoiceItemsMap[referenceId]) {
                // Update existing invoice item
                invoiceItemsMap[referenceId].quantity += quantity;

                // Recalculate unit price based on new quantities
                const totalInvoiceLinePrice = invoiceItemsMap[referenceId].displayPrice * invoiceItemsMap[referenceId].quantity;
                const totalInvoiceLineTax = invoiceItemsMap[referenceId].displayTax * invoiceItemsMap[referenceId].quantity;

                invoiceItemsMap[referenceId].displayPrice = parseFloat((totalInvoiceLinePrice / invoiceItemsMap[referenceId].quantity).toFixed(2));
                invoiceItemsMap[referenceId].displayTax = parseFloat((totalInvoiceLineTax / invoiceItemsMap[referenceId].quantity).toFixed(2));
            } else {
                // Create new invoice item
                invoiceItemsMap[referenceId] = {
                    "referenceType": "item",
                    "referenceId": referenceId,
                    "itemName": nameLookup[sku],
                    "quantity": quantity,
                    "displayPrice": displayPrice,
                    "displayTax": displayTax
                };
            }

            // Update total payment amount
            orderJSON.invoices[0].payments[0].amount += displayLineTotal;
        }

        // Now convert maps to arrays and push to orderJSON
        orderJSON.items = Object.values(itemsMap);
        orderJSON.invoices[0].items = Object.values(invoiceItemsMap);

        try {
            if (orderJSON.items.length > 0){
                await common.requester('post', `https://api.stok.ly/v2/saleorders`, orderJSON, 0)
                await common.sleep(1000)
            }
        } catch (e) {
            console.log(order)
            console.log(e)
            // await common.askQuestion('PAUSE')
        }


    }

}

function lookupUser(operator) {
	const operatorCode = operator.substring(0, 2)
	return userMap[operatorCode]
}

async function run() {
    for (const channel of Object.keys(channelDetails)) {
        await getOrdersSheet(channel)
    }
    await getAllOrders()
    await getAllItems()


    await processOrders()

}

module.exports = {
    run
}