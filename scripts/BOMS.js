let common = require('../common.js');
const convertCSV = require("json-2-csv");
const fs = require('fs');
const csv = require('fast-csv');
const path = require('path');
process.chdir(__dirname);
let bomFolderPath = '../files - new/BOM/details';
let bomDetailsFolderPath = '../files - new/RM';
let bomCostsFolderPath = '../files - new/cost/Addtional_Element';
let supplierSheet = '../files - new/sage stuff/suppliers.csv';
let productFolderPath = '../files - new/product';
let bomRawCostsFolderPath = '../files - new/cost/header';

let supplierLookup = {};
let BOMS = {};
let BomDetailsLookup = {};
let itemList = {};
let itemTypeLookup = {};
let itemTypesToMake = [];
let BOMNumbers = {};
let simpleProducts = {};
let allCurrentBoms = [];
let suppliers = {};
let manufacturers = {};
let BOMRawCosts = {};
let itemIDLookup = {};
let itemListLookup = {};
let additionalCosts = {};

const defaultTaxClass = 'fa262a8d-1ebe-4959-98dd-6cca8dd32f61'

async function getProduct() {
    // Get all files in the product folder
    const files = await fs.promises.readdir(productFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} product CSV files in ${productFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const productFileName = path.join(productFolderPath, csvFile);
        console.log(`Processing product file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(productFileName)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing product file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    try {
                        const supplierCode = parseInt(row['preferred supplier code']);
                        console.log(supplierCode);
                        
                        if (manufacturers[supplierCode] == undefined) {
                            let contacts = [];
                            if (suppliers[supplierCode]['contact email'] != '' || suppliers[supplierCode]['contact telephone'] != '') {
                                contacts = [{
                                    "forename": suppliers[supplierCode]['short name'],
                                    "surname": "",
                                    "email": suppliers[supplierCode]['contact email'],
                                    "phone": suppliers[supplierCode]['contact telephone'],
                                    "tags": [],
                                    "name": {
                                        "forename": suppliers[supplierCode]['short name'],
                                        "surname": ""
                                    }
                                }];
                            }
                            
                            await common.requester('post', `https://api.stok.ly/v0/manufacturers`, {
                                "name": suppliers[supplierCode].name + ' - ' + suppliers[supplierCode].code,
                                "accountReference": suppliers[supplierCode]['default nominal account number'],
                                "vatNumber": suppliers[supplierCode]['tax registration number'] != '' ? {
                                    "value": suppliers[supplierCode]['tax registration number'],
                                    "country": suppliers[supplierCode]['country code']
                                } : undefined,
                                "currency": suppliers[supplierCode]['contact email'].currency,
                                contacts: contacts,
                                "addresses": [
                                    {
                                        "line1": suppliers[supplierCode]['address line1'].length > 2 ? suppliers[supplierCode]['address line1'] : 'undefined',
                                        "line2": suppliers[supplierCode]['address line2'] > 2 ? suppliers[supplierCode]['address line2'] : '',
                                        "city": suppliers[supplierCode]['city'] > 2 ? suppliers[supplierCode]['city'] : 'undefined',
                                        "region": suppliers[supplierCode]['county'] > 2 ? suppliers[supplierCode]['county'] : '',
                                        "country": suppliers[supplierCode]['country code'],
                                        "postcode": "undefined"
                                    }
                                ]
                            }).then(r => {
                                manufacturers[supplierCode] = r.data.data.id;
                            });
                        }
                        
                        supplierLookup[row['product code']] = manufacturers[parseInt(row['preferred supplier code'])];
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all product files');
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

async function getBomDetails() {
    // Get all files in the BOM details folder
    const files = await fs.promises.readdir(bomDetailsFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM details CSV files in ${bomDetailsFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMDetailsName = path.join(bomDetailsFolderPath, csvFile);
        console.log(`Processing BOM details file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMDetailsName)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing BOM details file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', row => {
                    stream.pause();
                    
                    try {
                        if (BomDetailsLookup[row['rm code']] == undefined) {
                            BomDetailsLookup[row['rm code']] = {};
                        }
                        BomDetailsLookup[row['rm code']].type = row['rm type description'];
                        if (!itemTypesToMake.includes(row['rm type description'])) {
                            itemTypesToMake.push(row['rm type description']);
                        }
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all BOM details files');
    console.log(`Total unique RM codes processed: ${Object.keys(BomDetailsLookup).length}`);
    console.log(`Item types to make: ${itemTypesToMake.join(', ')}`);
}


async function getBomNumbers() {
    // Get all files in the BOM numbers folder
    const files = await fs.promises.readdir(bomFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM number CSV files in ${bomFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMFileName = path.join(bomFolderPath, csvFile);
        console.log(`Processing BOM number file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMFileName)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing BOM number file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', row => {
                    stream.pause();
                    
                    try {
                        if (BOMNumbers[row['product code']] == undefined) {
                            BOMNumbers[row['product code']] = {};
                        }
                        
                        if (BOMNumbers[row['product code']][row['product colour code']] == undefined) {
                            BOMNumbers[row['product code']][row['product colour code']] = parseInt(row['bom code']);
                        }
                        
                        if (BOMNumbers[row['product code']][row['product colour code']] < parseInt(row['bom code'])) {
                            BOMNumbers[row['product code']][row['product colour code']] = parseInt(row['bom code']);
                        }
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all BOM number files');
    
    // Calculate some statistics for logging
    let totalProducts = 0;
    let totalProductColorCombinations = 0;
    
    for (const productCode in BOMNumbers) {
        totalProducts++;
        totalProductColorCombinations += Object.keys(BOMNumbers[productCode]).length;
    }
    
}

async function getBomRawCosts() {
    // Get all files in the BOM raw costs folder
    const files = await fs.promises.readdir(bomRawCostsFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM raw costs CSV files in ${bomRawCostsFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMRawCostsCSV = path.join(bomRawCostsFolderPath, csvFile);
        console.log(`Processing BOM raw costs file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMRawCostsCSV)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing BOM raw costs file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', row => {
                    stream.pause();
                    
                    try {
                        BOMRawCosts[`${row['product/rm code']}_${row['colour code']}`] = row['product purchase price'];
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all BOM raw costs files');
    console.log(`Total unique product/color combinations with costs: ${Object.keys(BOMRawCosts).length}`);
}

async function getBomCosts() {
    // Get all files in the BOM costs folder
    const files = await fs.promises.readdir(bomCostsFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM costs CSV files in ${bomCostsFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMCosts = path.join(bomCostsFolderPath, csvFile);
        console.log(`Processing BOM costs file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMCosts)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing BOM costs file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    try {
                        if (additionalCosts[`${row['product/rm code']}_${row['colour code']}`] == undefined) {
                            additionalCosts[`${row['product/rm code']}_${row['colour code']}`] = [];
                        }
                        
                        additionalCosts[`${row['product/rm code']}_${row['colour code']}`].push({
                            name: row['additional cost name'],
                            amount: row['value'],
                            type: row.type
                        });
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all BOM costs files');
    
    // Calculate some statistics for logging
    let totalProductColorCombinations = Object.keys(additionalCosts).length;
    let totalAdditionalCosts = 0;
    
    for (const key in additionalCosts) {
        totalAdditionalCosts += additionalCosts[key].length;
    }
    
    console.log(`Total unique product/color combinations with additional costs: ${totalProductColorCombinations}`);
    console.log(`Total additional cost entries: ${totalAdditionalCosts}`);
}

async function getBoms() {
    // Track products without specific color code across all files
    let all = {};
    
    // Get all files in the BOM folder
    const files = await fs.promises.readdir(bomFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM CSV files in ${bomFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMFileName = path.join(bomFolderPath, csvFile);
        console.log(`Processing BOM file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMFileName)
                .pipe(csv.parse({
                    headers: headers => headers.map(h => h.toLowerCase().trim())
                }))
                .on('error', error => {
                    console.error(`Error processing BOM file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    try {
                        if (BOMNumbers[row['product code']] && 
                            BOMNumbers[row['product code']][row['product colour code']] == row['bom code']) {
                            
                            if (BOMS[`${row['product code']}`] == undefined) {
                                BOMS[`${row['product code']}`] = {};
                            }
                            
                            let paddedColourCode = (() => {
                                if (!isNaN(row['rm colour code'])) {
                                    return row['rm colour code'].padStart(4, '0');
                                }
                                return row['rm colour code'];
                            })();
                            
                            let product = {
                                sku: [row['rm code'], paddedColourCode, row['rm size code']].join('-'),
                                label: row['bom line name'],
                                amount: parseFloat(row['rm usage']),
                                type: BomDetailsLookup[row['rm code']]?.type || 'unknown'
                            };
                            
                            if (row['product colour code'].trim() != '') {
                                if (BOMS[`${row['product code']}`][row['product colour code']] == undefined) {
                                    BOMS[`${row['product code']}`][row['product colour code']] = [];
                                }
                                
                                let alreadyIn = false;
                                for (const item in BOMS[`${row['product code']}`][row['product colour code']]) {
                                    if (BOMS[`${row['product code']}`][row['product colour code']][item].sku == [row['rm code'], paddedColourCode, row['rm size code']].join('-')) {
                                        BOMS[`${row['product code']}`][row['product colour code']][item].amount += parseFloat(row['rm usage']);
                                        alreadyIn = true;
                                    }
                                }
                                
                                if (!alreadyIn) {
                                    BOMS[`${row['product code']}`][row['product colour code']].push(product);
                                }
                            } else {
                                if (all[row['product code']] == undefined) {
                                    all[row['product code']] = [];
                                }
                                
                                // Check if the item is already in the 'all' array
                                let alreadyInAll = false;
                                for (const item in all[row['product code']]) {
                                    if (all[row['product code']][item].sku == product.sku) {
                                        all[row['product code']][item].amount += product.amount;
                                        alreadyInAll = true;
                                        break;
                                    }
                                }
                                
                                if (!alreadyInAll) {
                                    all[row['product code']].push(product);
                                }
                            }
                        }
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', async () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    // Process the combined 'all' items
    await common.sleep(200);
    console.log('Processing color-agnostic BOM items...');
    
    for (const item of Object.keys(all)) {
        if (BOMS[item]) {
            for (const parent of Object.keys(BOMS[item])) {
                BOMS[item][parent].push(...all[item]);
            }
        }
    }
    
    console.log('Finished processing all BOM files');
    
    // Calculate some statistics for logging
    let totalProducts = Object.keys(BOMS).length;
    let totalProductColorCombinations = 0;
    let totalBOMItems = 0;
    
    for (const productCode in BOMS) {
        totalProductColorCombinations += Object.keys(BOMS[productCode]).length;
        
        for (const colorCode in BOMS[productCode]) {
            totalBOMItems += BOMS[productCode][colorCode].length;
        }
    }
    
    console.log(`Total products with BOMs: ${totalProducts}`);
    console.log(`Total product-color combinations: ${totalProductColorCombinations}`);
    console.log(`Total BOM items: ${totalBOMItems}`);
}


async function getAllVariables(){
    await common.loopThrough('Getting Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{2}))%26%26([status]!={1})%26%26[sku]::{@string;LILEBLZ_50942}', async (item)=>{
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
    await common.loopThrough('Getting Simple Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{0}))%26%26([status]!={1})%26%26[tags]::{RM}', async (item)=>{
        simpleProducts[item.name.toLowerCase().trim()] = item.itemId
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
            try{
                let manufacturingCostsParent = [{
                    "manufacturerId": supplierLookup[BOM],
                    "taxClassId": defaultTaxClass,
                    "name": `Cost of Item => ${fabric}`,
                    "cost": BOMRawCosts[`${BOM}_${fabric}`],
                    "tax": 0,
                    "rate": 0,
                    automaticallyIncludeOnManufacturingRuns: true
                }]
    
                for (const additional of additionalCosts[`${BOM}_${fabric}`]){
                    manufacturingCostsParent.push({
                        "manufacturerId": supplierLookup[BOM],
                        "taxClassId": defaultTaxClass,
                        "name": `${additional.name} => ${fabric}`,
                        "cost": additional.type.toLowerCase() == 'percentage' ? BOMRawCosts[`${BOM}_${fabric}`] * (additional.amount/100) : additional.amount,
                        "tax": 0,
                        "rate": 0,
                        automaticallyIncludeOnManufacturingRuns: false
                    })
                }
                for (const item of itemList[`${BOM}_${fabric}`]){

                    let manufacturingCostsChild = [{
                        "manufacturerId": supplierLookup[BOM],
                        "taxClassId": defaultTaxClass,
                        "name": `Cost of Item => ${itemListLookup[item]}`,
                        "cost": BOMRawCosts[`${BOM}_${fabric}`],
                        "tax": 0,
                        "rate": 0,
                        automaticallyIncludeOnManufacturingRuns: true
                    }]
        
                    for (const additional of additionalCosts[`${BOM}_${fabric}`]){
                        manufacturingCostsChild.push({
                            "manufacturerId": supplierLookup[BOM],
                            "taxClassId": defaultTaxClass,
                            "name": `${additional.name} => ${itemListLookup[item]}`,
                            "cost": additional.type.toLowerCase() == 'percentage' ? BOMRawCosts[`${BOM}_${fabric}`] * (additional.amount/100) : additional.amount,
                            "tax": 0,
                            "rate": 0,
                            automaticallyIncludeOnManufacturingRuns: false
                        })
                    }

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
                            "quantity": parseFloat(RM.amount.toFixed(4))
                        })
        
                    }
        
                    let attempts = 0;
                    const maxRetries = 5;
                    while (attempts < maxRetries) {
                        
                        try {
                            await common.requester('patch', `https://api.stok.ly/v0/items/${item}`, {
                                "acquisition": 2,
                                "billOfMaterials": bomList,
                                manufacturingCosts: manufacturingCostsChild
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
                            manufacturingCosts: manufacturingCostsParent
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

    fs.writeFileSync('./testBOMs.txt', JSON.stringify(BOMS))
    console.log(itemList)
}

module.exports = {
    run
}