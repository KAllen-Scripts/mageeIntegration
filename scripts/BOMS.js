let common = require('../common.js');
const convertCSV = require("json-2-csv");
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);
let BOMFileName = '../files/BOMS/BOM_details_20241216145952.csv';
let BOMDetailsName = '../files/BOMS/RW_20241216154332.csv';
let BOMCosts = '../files/BOMS/Cost_details_Additional_20241216153820.csv';
let supplierSheet = '../files/manufacturing/suppliers.csv';
let productFileName = '../files/items/Product_20241216154235.csv';
let BOMRawCostsCSV = '../files/BOMS/Cost_20241216153820(in).csv';

let supplierLookup = {};
let BOMS = {};
let BomDetailsLookup = {};
let itemList = {};
let itemTypeLookup = {};
let itemTypesToMake = [];
let BOMNumbers = {};
let simpleProducts = {};
let allCurrentBoms = []
let suppliers = {}
let manufacturers = {}
let BOMRawCosts = {}
let itemIDLookup = {}
let itemListLookup = {}
let additionalCosts = {}

async function getProduct(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(productFileName)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  async row => {
            stream.pause()

            console.log(parseInt(row['preferred supplier code']))

                if (manufacturers[parseInt(row['preferred supplier code'])] == undefined){

                    let contacts = []
                    if (suppliers[parseInt(row['preferred supplier code'])]['contact email'] != '' || suppliers[parseInt(row['preferred supplier code'])]['contact telephone'] != ''){
                        contacts = [{
                            "forename": suppliers[parseInt(row['preferred supplier code'])]['short name'],
                            "surname": "",
                            "email": suppliers[parseInt(row['preferred supplier code'])]['contact email'],
                            "phone": suppliers[parseInt(row['preferred supplier code'])]['contact telephone'],
                            "tags": [],
                            "name": {
                                "forename": suppliers[parseInt(row['preferred supplier code'])]['short name'],
                                "surname": ""
                            }
                        }]
                    }

                    await common.requester('post', `https://api.stok.ly/v0/manufacturers`, {
                        "name": suppliers[parseInt(row['preferred supplier code'])].name + ' - ' + suppliers[parseInt(row['preferred supplier code'])].code,
                        "accountReference": suppliers[parseInt(row['preferred supplier code'])]['default nominal account number'],
                        "vatNumber": suppliers[parseInt(row['preferred supplier code'])]['tax registration number'] != '' ? {
                            "value": suppliers[parseInt(row['preferred supplier code'])]['tax registration number'],
                            "country": suppliers[parseInt(row['preferred supplier code'])]['country code']
                        } : undefined,
                        "currency": suppliers[parseInt(row['preferred supplier code'])]['contact email'].currency,
                        contacts: contacts,
                        "addresses": [
                            {
                                "line1": suppliers[parseInt(row['preferred supplier code'])]['address line1'].length > 2 ? suppliers[parseInt(row['preferred supplier code'])]['address line1'] : 'undefined',
                                "line2": suppliers[parseInt(row['preferred supplier code'])]['address line2'] > 2 ? suppliers[parseInt(row['preferred supplier code'])]['address line2'] : '',
                                "city": suppliers[parseInt(row['preferred supplier code'])]['city'] > 2 ? suppliers[parseInt(row['preferred supplier code'])]['city'] : 'undefined',
                                "region": suppliers[parseInt(row['preferred supplier code'])]['county'] > 2 ? suppliers[parseInt(row['preferred supplier code'])]['county'] : '',
                                "country": suppliers[parseInt(row['preferred supplier code'])]['country code'],
                                "postcode": "undefined"
                            }
                        ]
                    }).then(r=>{
                        manufacturers[parseInt(row['preferred supplier code'])] = r.data.data.id
                    })
                }

                supplierLookup[row['product code']] = manufacturers[parseInt(row['preferred supplier code'])] 

            stream.resume()
        })
        .on('end', () => {
            res()
        })
    })
}

async function getAllManufacturers(){
    await common.loopThrough('Getting Manufacturers', `https://api.stok.ly/v0/manufacturers`, 'size=1000', '[status]=={active}', async (m)=>{
        try{
            manufacturers[parseInt(m.name.split('-')[1].toLowerCase().trim())] = m.manufacturerId    
        } catch {}
    })
};

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

async function getBomDetails(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(BOMDetailsName)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()

                if (BomDetailsLookup[row['rm code']] == undefined){BomDetailsLookup[row['rm code']] = {}}
                BomDetailsLookup[row['rm code']].type = row['rm type description']
                if(!itemTypesToMake.includes(row['rm type description'])){itemTypesToMake.push(row['rm type description'])}

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}


async function getBomNumbers(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(BOMFileName)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()

                if (BOMNumbers[row['product code']] == undefined){BOMNumbers[row['product code']] = {}}
                if (BOMNumbers[row['product code']][row['product colour code']] == undefined){BOMNumbers[row['product code']][row['product colour code']] = parseInt(row['bom code'])}
                if (BOMNumbers[row['product code']][row['product colour code']] < parseInt(row['bom code'])){BOMNumbers[row['product code']][row['product colour code']] = parseInt(row['bom code'])}

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function getBomRawCosts(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(BOMRawCostsCSV)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()

                BOMRawCosts[`${row['product/rm code']}_${row['colour code']}`] = row['product purchase price']

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function getBomCosts(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(BOMCosts)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  async row => {
            stream.pause()

            try{
                if(additionalCosts[`${row['product/rm code']}_${row['colour code']}`] == undefined){additionalCosts[`${row['product/rm code']}_${row['colour code']}`] = []}
                additionalCosts[`${row['product/rm code']}_${row['colour code']}`].push({
                    name: row['additional cost name'],
                    amount: row['value'],
                    type: row.type
                })
            } catch {}

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function getBoms() {
    let all = {}
    return new Promise((res, rej) => {
        const stream = fs.createReadStream(BOMFileName)
            .pipe(csv.parse({
                headers: headers => headers.map(h => h.toLowerCase().trim())
            }))
            // .on('error', error => console.error(error))
            .on('data', async row => {
                stream.pause()
                if (BOMNumbers[row['product code']][row['product colour code']] == row['bom code']) {
                    if (BOMS[`${row['product code']}`] == undefined) {
                        BOMS[`${row['product code']}`] = {}
                    }

                    let paddedColourCode = (() => {
                        if (!isNaN(row['rm colour code'])) {
                            return row['rm colour code'].padStart(4, '0')
                        }
                        return row['rm colour code']
                    })()

                    let product = {
                        sku: [row['rm code'], paddedColourCode, row['rm size code']].join('-'),
                        label: row['bom line name'],
                        amount: parseFloat(row['rm usage']),
                        type: BomDetailsLookup[row['rm code']].type
                    }

                    if (row['product colour code'].trim() != ''){
                        if (BOMS[`${row['product code']}`][row['product colour code']] == undefined) {
                            BOMS[`${row['product code']}`][row['product colour code']] = []
                        }
                        let alreadyIn = false
                        for (const item in BOMS[`${row['product code']}`][row['product colour code']]){
                            if (BOMS[`${row['product code']}`][row['product colour code']][item].sku == [row['rm code'], paddedColourCode, row['rm size code']].join('-')){
                                BOMS[`${row['product code']}`][row['product colour code']][item].amount += parseFloat(row['rm usage'])
                                alreadyIn = true
                            }
                        }
    
                        if (!alreadyIn){
                            BOMS[`${row['product code']}`][row['product colour code']].push(product)
                        }
                    } else {
                        if(all[row['product code']] == undefined){all[row['product code']] = []}
                        all[row['product code']].push(product)
                    }

                    // await common.askQuestion(2)
                }
                stream.resume()
            })
            .on('end', async () => {
                await common.sleep(200)
                for(const item of Object.keys(all)){
                    for (const parent of Object.keys(BOMS[item])){
                        BOMS[item][parent].push(...all[item])
                    }
                }
                res()
            })
    })
}


async function getAllVariables(){
    await common.loopThrough('Getting Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{2}))%26%26([status]!={1})', async (item)=>{
        itemIDLookup[item.sku.toLowerCase().trim()] = item.itemId
        itemList[item.sku] = []
        await common.loopThrough('', `https://api.stok.ly/v0/items/${item.itemId}/children`, 'size=1000', '([status]!={1})', async (childItem)=>{
            itemListLookup[childItem.itemId] = childItem.sku
            itemList[item.sku].push(childItem.itemId)
        })
    })
};

async function getAllCurrentBOMS(){
    await common.loopThrough('Getting BOM Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{3}))%26%26([status]!={1})', async (item)=>{
        allCurrentBoms.push(item.itemId)
    })
};

async function getAllSimples(){
    await common.loopThrough('Getting Simple Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{0}))%26%26([status]!={1})', async (item)=>{
        simpleProducts[item.sku.toLowerCase().trim()] = item.itemId
    })
};


async function getAllTypes(){
    await common.loopThrough('Getting Types', `https://api.stok.ly/v0/item-types`, 'size=1000&sortDirection=ASC&sortField=name', '[status]!={1}', async (type)=>{
        itemTypeLookup[type.name.toLowerCase()] = type.itemTypeId
    })
};

async function makeItemTypes(){
    for (const type of itemTypesToMake){
        if (itemTypeLookup[type.toLowerCase()] == undefined){
            await common.requester('post', 'https://api.stok.ly/v0/item-types', {"name": type}).then(r=>{
                itemTypeLookup[type.toLowerCase()] = r.data.data.id
            })
        }
    }
    
}

async function makeBoms(){

    for (const BOM of Object.keys(BOMS)){
        for (const fabric of Object.keys(BOMS[BOM])){
            let templateList = []
            let billOfMaterialsTemplate = []
            // if(`${BOM}_${fabric}` != 'LISL2PC_43414'){continue}
            try{
                let manufacturingCosts = [{
                    "manufacturerId": supplierLookup[BOM],
                    "taxClassId": "838fee51-9c0f-41ad-9b1b-2846bdb0c872",
                    "name": `Cost of Item => ${itemListLookup[item]}`,
                    "cost": BOMRawCosts[`${BOM}_${fabric}`],
                    "tax": 0,
                    "rate": 0,
                    automaticallyIncludeOnManufacturingRuns: true
                }]
    
                for (const additional of additionalCosts[`${BOM}_${fabric}`]){
                    manufacturingCosts.push({
                        "manufacturerId": supplierLookup[BOM],
                        "taxClassId": "838fee51-9c0f-41ad-9b1b-2846bdb0c872",
                        "name": `${additional.name} => ${itemListLookup[item]}`,
                        "cost": additional.type.toLowerCase() == 'percentage' ? BOMRawCosts[`${BOM}_${fabric}`] * (additional.amount/100) : additional.amount,
                        "tax": 0,
                        "rate": 0,
                        automaticallyIncludeOnManufacturingRuns: false
                    })
                }
                for (const item of itemList[`${BOM}_${fabric}`]){
                    let bomList = []
                    for (const RM of BOMS[BOM][fabric]){
                        if(!templateList.includes(simpleProducts[RM.sku.toLowerCase().trim()])){
                            templateList.push(simpleProducts[RM.sku.toLowerCase().trim()])
                            billOfMaterialsTemplate.push({
                                "label": RM.label,
                                "predicate": `([typeId]=={${itemTypeLookup[RM.type.toLowerCase()]}})`,
                                "sortIndex": billOfMaterialsTemplate.length
                            })
                        }
                        
        
                        bomList.push({
                            "itemId": simpleProducts[RM.sku.toLowerCase().trim()],
                            "billOfMaterialItemId": simpleProducts[RM.sku.toLowerCase().trim()],
                            "label": RM.label,
                            "quantity": RM.amount
                        })
        
                    }
        
                    let attempts = 0;
                    const maxRetries = 5;
                    while (attempts < maxRetries) {
                        
                        try {
                            await common.requester('patch', `https://api.stok.ly/v0/items/${item}`, {
                                "acquisition": 2,
                                "billOfMaterials": bomList,
                                manufacturingCosts
                            }, 0);
                            break
                        } catch (error) {
                            attempts ++
                            console.log(error)
                        }
                    }
                    console.log(`Done item ${item}`)
                }
                let attempts = 0;
                const maxRetries = 5;
                while (attempts < maxRetries) {
                    try {
                        await common.requester('patch', `https://api.stok.ly/v0/variable-items/${itemIDLookup[`${BOM}_${fabric}`.toLowerCase().trim()]}`, {
                            billOfMaterialsTemplate,
                            manufacturingCosts
                        }, 0);
                        break
                    } catch (error) {
                        attempts++;
                        console.log(error)
                    }
                }
            }catch{}
        }
    }

}

async function run(){
    await getAllManufacturers()
    await getCustomersSheet()
    await getProduct()
    await getAllCurrentBOMS()
    await getBomNumbers()
    await getAllSimples()
    await getBomDetails()
    await getAllVariables()
    await getAllTypes()
    await getBoms()
    await makeItemTypes()
    await getBomRawCosts()
    await getBomCosts()

    await makeBoms()
}

module.exports = {
    run
}