let common = require('../common.js');
const fs = require('fs');
const fastcsv = require('fast-csv');
const path = require('path');
process.chdir(__dirname);
let RMSku = '../files - new/rm sku';
let RMData = '../files - new/rm';
let RMCost = '../files - new/cost/header';

let rawMaterials = {};
let suppliers = {};
let costLookup = {};
let itemTypes = {};

let supplierCustom

let multiplicationExceptions = ['Cloth', 'Fully Factored'];
let altSKUExceptions = ['Button','Lining','Waistband/Pocket','Alcantara','Melton','Woven Label','Ticket/Story','Sundry']

async function getSuppliers(){
    await common.loopThrough('Getting Suppliers', `https://${global.enviroment}/v1/suppliers`, 'size=1000', '[status]!={1}', (supp)=>{
        suppliers[supp.accountReference.toLowerCase().trim()] = supp.supplierId
        if (supp.accountReference == 'CUST'){supplierCustom = supp.supplierId}
    })
};

async function getItemTypes(){
    await common.loopThrough('Getting Types', `https://${global.enviroment}/v0/item-types`, 'size=1000', '[status]!={1}', (type)=>{
        itemTypes[type.name.toLowerCase().trim()] = type.itemTypeId
    })
};

async function processRMCosts() {
    // Get all CSV files in the folder
    const files = await fs.promises.readdir(RMCost);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} CSV files in ${RMCost}`);
    
    // Process each CSV file sequentially
    for (const csvFile of csvFiles) {
        const RMCostCSV = path.join(RMCost, csvFile);
        console.log(`Processing ${csvFile}...`);
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(RMCostCSV)
                .pipe(fastcsv.parse({ headers: true, trim: true }))
                .on('error', error => {
                    console.error('Error parsing CSV:', error);
                    reject(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    let key = row['Product/RM Code'] || '';
                    if (row['Colour Code'] && row['Colour Code'].trim() != '') {
                        key += `-${row['Colour Code']}`;
                    }
                    if(row['Colour Code'] == ''){
                        key += `-${row['Product/RM Code']}`
                    }
                    
                    if (key && row['Product Purchase Price'] !== undefined) {
                        costLookup[key] = parseFloat(row['Product Purchase Price']);
                    }

                    stream.resume();
                })
                .on('end', async () => {
                    resolve();
                });
        });
    }
}

async function processRMSKUCSV() {
    // Get all CSV files in the folder
    const files = await fs.promises.readdir(RMSku);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} CSV files in ${RMSku}`);
    
    // Process each CSV file sequentially
    for (const csvFile of csvFiles) {
        const RMCSV = path.join(RMSku, csvFile);
        console.log(`Processing ${csvFile}...`);
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(RMCSV)
                .pipe(fastcsv.parse({ headers: true, trim: true }))
                .on('error', error => {
                    console.error('Error parsing CSV:', error);
                    reject(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    if (rawMaterials[row.SKU] == undefined) {
                        let colorRef = row.RM || '';
                        if (row['Colour Code']) {
                            colorRef += `-${row['Colour Code']}`;
                        }
                        
                        rawMaterials[row.SKU] = {
                            potentialAltSku: row['RM Name'],
                            colorRef: colorRef
                        }
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    resolve();
                });
        });
    }
}



async function processRMData() {
    // Get all CSV files in the folder
    const files = await fs.promises.readdir(RMData);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} CSV files in ${RMData}`);
    
    // Process each CSV file sequentially
    for (const csvFile of csvFiles) {
        const RMDataCSV = path.join(RMData, csvFile);
        console.log(`Processing ${csvFile}...`);
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(RMDataCSV)
                .pipe(fastcsv.parse({ headers: true, trim: true }))
                .on('error', error => {
                    console.error('Error parsing CSV:', error);
                    reject(error);
                })
                .on('data', async row => {
                    stream.pause();
                    

                    for (const RM of Object.keys(rawMaterials)){
                        try {
                            let compareString = row['RM Code'] || '';
                            if (row['RM Colour Code']) {
                                compareString += `-${row['RM Colour Code']}`;
                            }

                            if (rawMaterials[RM].colorRef == compareString) {
                                rawMaterials[RM].type = row['RM Type Description'];

                                let amount = (multiplicationExceptions.includes(row['RM Type Description']) ? costLookup[`${row['RM Code']}-${row['RM Colour Code']}`] : costLookup[`${row['RM Code']}-${row['RM Colour Code']}`] * 100) || 0

                                if (costLookup[`${row['RM Code']}-${row['RM Colour Code']}`] < 0.001){amount = 0}

                                rawMaterials[RM].UOM = [
                                    {
                                        "supplierId": suppliers[isNaN(parseInt(row['Preferred Supplier Code'])) ? row['Preferred Supplier Code'].toLowerCase().trim() : row['Preferred Supplier Code'].replace(/^0+/, '')] || supplierCustom,
                                        "supplierSku": altSKUExceptions.includes(row['RM Type Description']) ? RM : rawMaterials[RM].potentialAltSku,
                                        "cost": {
                                            "amount": parseFloat(amount.toFixed(2)),
                                            "currency": "GBP"
                                        },
                                        "currency": "GBP",
                                        "quantityInUnit": multiplicationExceptions.includes(row['RM Type Description']) ? 1 : 100
                                    }
                                ];
                            }
                        } catch {}
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    resolve();
                });
        });
    }
    
}

let dupeList = []
async function makeRMs(){
    for (const RM of Object.keys(rawMaterials)){
        // if (RM != '62011-62011-STD'){continue}
        try{
            let type = rawMaterials[RM].type
            if (!type || itemTypes?.[type.toLowerCase().trim()] == undefined){
                await common.requester('post', `https://${global.enviroment}/v0/item-types`, {"name": type}).then(r=>{
                    itemTypes[type.toLowerCase().trim()] = r.data.data.id
                })
            }

            let sku = (() => {
                const itemToCheck = altSKUExceptions.includes(type) ? RM : rawMaterials[RM].potentialAltSku;
                const occurrences = dupeList.filter(item => item === itemToCheck).length;
                if (dupeList.includes(itemToCheck)) {
                    return itemToCheck + ` - ${occurrences}`
                }
                return itemToCheck
            })();

            console.log({
                tags: ['RM'],
                "format": 0,
                "name": RM,
                "sku": sku,
                typeId: itemTypes[type.toLowerCase().trim()],
                unitsOfMeasure: rawMaterials[RM].UOM
            })
    
            dupeList.push(altSKUExceptions.includes(type) ? RM : rawMaterials[RM].potentialAltSku)

            await common.requester('post', `https://${global.enviroment}/v0/items`, {
                tags: ['RM'],
                "format": 0,
                "name": RM,
                "sku": sku,
                typeId: itemTypes[type.toLowerCase().trim()],
                unitsOfMeasure: rawMaterials[RM].UOM
            })

        } catch (e) {
            console.log(e)
            // await common.askQuestion(1)
        }


    }
}


async function run() {

    await getItemTypes()
    await processRMCosts()
    await getSuppliers()
    await processRMSKUCSV()
    await processRMData()

    await makeRMs()
        
}

module.exports = {
    run
};