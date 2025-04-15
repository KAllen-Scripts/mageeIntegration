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

let multiplicationExceptions = ['Cloth', 'Fully Factored'];
let altSKUExceptions = ['Button', 'Fully Factored']

async function getSuppliers(){
    await common.loopThrough('Getting Suppliers', `https://${global.enviroment}/v1/suppliers`, 'size=1000', '[status]!={1}', (supp)=>{
        suppliers[supp.accountReference.toLowerCase().trim()] = supp.supplierId
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

                    console.log(row['Product Purchase Price'])
                    
                    if (row['Product/RM Code'] && row['Colour Code'] && 
                        row['Product/RM Code'] !== '' && row['Colour Code'] !== '' && 
                        row['Product Purchase Price'] !== undefined) {
                        
                        costLookup[`${row['Product/RM Code']}-${row['Colour Code']}`] = row['Product Purchase Price'];
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
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
                    
                    if (rawMaterials[row.SKU] == undefined){
                        rawMaterials[row.SKU] = {
                            potentialAltSku: row['RM Name'],
                            colorRef: `${row.RM}-${row['Colour Code']}`
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
                    
                    console.log(`${row['RM Code']}-${row['RM Colour Code']}`)

                    for (const RM of Object.keys(rawMaterials)){
                        try{
                            if (rawMaterials[RM].colorRef == `${row['RM Code']}-${row['RM Colour Code']}`){
                                rawMaterials[RM].type = row['RM Type Description']
                                rawMaterials[RM].UOM = [
                                    {
                                        "supplierId": suppliers[isNaN(parseInt(row['Preferred Supplier Code'])) ? row['Preferred Supplier Code'] : row['Preferred Supplier Code'].replace(/^0+/, '')],
                                        "supplierSku": altSKUExceptions.includes(row['RM Type Description']) ? RM : rawMaterials[RM].potentialAltSku,
                                        "cost": {
                                            "amount": multiplicationExceptions.includes(row['RM Type Description']) ? RMCost[`${row['RM Code']}-${row['RM Colour Code']}`] : RMCost[`${row['RM Code']}-${row['RM Colour Code']}`] * 100,
                                            "currency": "GBP"
                                        },
                                        "currency": "GBP",
                                        "quantityInUnit": multiplicationExceptions.includes(row['RM Type Description']) ? 1 : 100
                                    }
                                ]
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

async function makeRMs(){
    for (const RM of Object.keys(rawMaterials)){

        try{
            let type = rawMaterials[RM].type
            if (!type || itemTypes?.[type.toLowerCase().trim()] == undefined){
                await common.requester('post', `https://${global.enviroment}/v0/item-types`, {"name": type}).then(r=>{
                    itemTypes[type.toLowerCase().trim()] = r.data.data.id
                })
            }

            console.log({
                "format": 0,
                "name": RM,
                "sku": altSKUExceptions.includes(type) ? RM : rawMaterials[RM].potentialAltSku,
                typeId: itemTypes[type],
                unitsOfMeasure: rawMaterials[RM].UOM
            })

            await common.askQuestion(1)
    
            // await common.requester('post', `https://${global.enviroment}/v0/items`, {
            //     "format": 0,
            //     "name": RM,
            //     "sku": altSKUExceptions.includes(type ? RM : rawMaterials[RM].potentialAltSku),
            //     typeId: itemTypes[type],
            //     unitsOfMeasure: rawMaterials[RM].UOM
            // })
        } catch (e) {
            console.log(e)
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

    console.log(JSON.stringify(rawMaterials))
        
}

module.exports = {
    run
};