let common = require('../common.js');
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);


// Input files
// let stockSheet = '../files - new/Clothing Retail branch stock @ 12th May 2025.csv';
let stockSheet = '../files - new/Clothing warehouse stock @ 12th May 2025.csv';

let stockList = {};
let itemSkuList = {};
let itemNameList = {};
let itemBarcodeList = {};
let currentInventory = {};

const locationLookup = {
    'ARNOTTS': 'aa383293-f340-4fc6-b021-9a3f7633d8a4',
    'DONEGAL': 'a74a39fe-9133-4dff-9522-a0c4e2526148',
    'SOUTH ANNE ST': '3ef8a574-19f2-4c6d-be2c-d68cb3fc97d9',
    'THE WEAVERS LOFT': 'ef536444-75c2-4af9-b44c-07179372d56f',
    'WEB': '55c142b5-f047-441b-8b06-47df61f94b67'
}

async function getItems() {
    await common.loopThrough(
        'Getting Items', 
        `https://${global.enviroment}/v0/items`, 
        'size=1000&sortDirection=ASC&sortField=timeUpdated', 
        `[status]!={1}%26%26[format]!={2}`, 
        async (item) => {
            itemSkuList[item.sku.toLowerCase().trim()] = item.itemId
            itemNameList[item.name.toLowerCase().trim()] = item.itemId
            try {
                for (const barcode of item.barcodes){
                    itemBarcodeList[barcode] = item.itemId
                }
            } catch {}
        }
    );
}

async function getInv() {
    await common.loopThrough(
        'Getting Inventory', 
        `https://${global.enviroment}/v1/inventory-records`, 
        'size=1000', 
        `[binId]=={UNASSIGNED}%26%26[onHand]=={0}`, 
        async (item) => {
            if (currentInventory[item.locationId] == undefined){currentInventory[item.locationId] = {}}
            currentInventory[item.locationId][item.itemId = item.onHand]
        }
    );
}

async function getIdentifier(row) {
    // Process the color value
    let colorValue = row.Colour;
    if (!isNaN(colorValue)) {
        // If it's a number, strip leading zeros
        colorValue = colorValue.toString().replace(/^0+/, '');
    }
    // Otherwise leave it as is

    // Build the variant SKU with the processed color value
    let variantSku = `${row.SPC}_${colorValue}-${row['Size'].replace('ONE SIZE', 'OS')}`.toLocaleLowerCase().trim()
    let variantSku2 = `${row.SPC}_${colorValue}-${row['Size']}`.toLocaleLowerCase().trim()
    
    // Check for matches in the various lookup tables
    if (row.ALT10 != 0 && itemBarcodeList[row.ALT10] != undefined) {
        return itemBarcodeList[row.ALT10]
    }
    if (row.GTIN != 0 && itemBarcodeList[row.GTIN] != undefined) {
        return itemBarcodeList[row.GTIN]
    }
    if (row['System Barcode'] != 0 && itemBarcodeList[row['System Barcode']] != undefined) {
        return itemBarcodeList[row['System Barcode']]
    }
    if (itemSkuList[variantSku] != undefined) {
        return itemSkuList[variantSku]
    }
    if (itemSkuList[variantSku2] != undefined) {
        return itemSkuList[variantSku2]
    }
    if (itemSkuList[row.SPC] != undefined) {
        return itemSkuList[row.SPC]
    }
}

async function processInventoryCSV() {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(stockSheet)
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
                
                    if (row.SPC.trim() != ''){
                        

                        let itemId = await getIdentifier(row)
                        if(itemId != undefined){
                            if (stockList[row.Branch] == undefined){stockList[row.Branch] = {}}
                            if (stockList[row.Branch][itemId] == undefined){stockList[row.Branch][itemId] = 0}
                            stockList[row.Branch][itemId] += parseFloat(row['Stock\nQty'])
                            console.log(stockList[row.Branch][itemId])
                        }
                    }
                
                stream.resume();
            })
            .on('end', () => {
                resolve();
            });
    });
}

async function processAdjustments() {
    try {
      // Validate inputs first
      if (!stockList || typeof stockList !== 'object') {
        throw new Error('stockList is undefined or not an object');
      }
      
      if (!locationLookup || typeof locationLookup !== 'object') {
        throw new Error('locationLookup is undefined or not an object');
      }
      
      if (!common || typeof common.requester !== 'function') {
        throw new Error('common.requester is undefined or not a function');
      }
  
      for (const location of Object.keys(stockList)) {
        try {
          // Validate location exists in lookup
          if (!locationLookup[location]) {
            console.error(`Location ${location} not found in locationLookup`);
            continue; // Skip this location but continue processing others
          }
  
          const locationId = locationLookup[location];
          
          // Error handling for bin lookup
          let binId;
          try {
            const binResponse = await common.requester(
              'get', 
              `https://api.stok.ly/v0/locations/${locationId}/bins?filter={default}=={1}`
            );
            
            if (!binResponse?.data?.data?.[0]?.binId) {
              throw new Error(`No default bin found for location ${location}`);
            }
            
            binId = binResponse.data.data[0].binId;
          } catch (binError) {
            console.error(`Failed to get bin for location ${location}: ${binError.message}`);
            // Use fallback or continue to next location
            continue;
          }
  
          let adjustPayload = {
            "locationId": locationId,
            "binId": binId,
            "reason": "Initial Import",
            "items": []
          };
          
          // Process each item in the location's stock list
          for (const item of Object.keys(stockList[location])) {
            try {
              // Fix the incorrect reference to stockList[location]
              // It should access the specific item quantity
              const currentStock = stockList[location][item] || 0;
              
              // Safely access current inventory with nullish coalescing
              const existingStock = (currentInventory?.[locationId]?.[item] || 0);
              
              // Calculate adjustment quantity
              const adjustmentQuantity = currentStock - existingStock;
              
              // Only add non-zero adjustments
              if (adjustmentQuantity !== 0) {
                adjustPayload.items.push({
                  "itemId": item,
                  "quantity": adjustmentQuantity
                });
              }
              
              // If we've reached the batch size limit, process the batch
              if (adjustPayload.items.length >= 200) {
                await common.requester('post', 'https://api.stok.ly/v1/adjustments', adjustPayload);
                // Reset items array after successful processing
                adjustPayload.items = [];
              }
            } catch (itemError) {
              console.error(`Error processing item ${item} for location ${location}: ${itemError.message}`);
              // Continue with next item
            }
          }
          
          // Process any remaining items
          if (adjustPayload.items.length > 0) {
            await common.requester('post', 'https://api.stok.ly/v1/adjustments', adjustPayload);
          }
        } catch (locationError) {
          console.error(`Error processing location ${location}: ${locationError.message}`);
          // Continue with next location
        }
      }
      
      return { success: true, message: "Stock adjustments processed successfully" };
    } catch (error) {
      console.error(`Fatal error in processAdjustments: ${error.message}`);
      // Rethrow or handle as needed by your application
      return { success: false, error: error.message };
    }
  }

async function run(){
    await getItems()
    await getInv()
    await processInventoryCSV()
    fs.writeFileSync('./inventory.txt', JSON.stringify(stockList))
    await processAdjustments()
    // console.log(stockList)
}

module.exports = {
    run
}