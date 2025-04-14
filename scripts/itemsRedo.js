const common = require('../common.js');
const fs = require('fs');
const path = require('path');
const fastcsv = require('fast-csv');
const { countries } = require('countries-list');

// File paths
const itemsFilePath = path.resolve(__dirname, '../files - new/data.json');
const imagesFilePath = path.resolve(__dirname, '../files - new/images.json');
const protexSKUFolder = path.resolve(__dirname, '../files - new/Product SKU');
const protexProductFolderPath = path.resolve(__dirname, '../files - new/product');
const fmFilePath = path.resolve(__dirname, '../files - new/FM/250225_FM Product Data - all skus 25.02.25.csv');
const siteProductsFilePath = path.resolve(__dirname, '../files - new/website/Models_11042025104320.csv');
const attributeNamesFilePath = path.resolve(__dirname, '../files - new/website/Attribute Names.csv');
const attributeValuesFilePath = path.resolve(__dirname, '../files - new/website/Attribute Values.csv');
const stockAttributesFilePath = path.resolve(__dirname, '../files - new/website/Stock Attributes.csv');
const modelImagesFilePath = path.resolve(__dirname, '../files - new/website/IRP-img-urls-all-time.csv');
const FMSupplierLookupCSV = path.resolve(__dirname, '../files - new/FM-supplier-code-lookup.csv');



//mageeTest mappings
///////////////////////////////////////////////////////////////////////////////////
// const childMappables = {
//     FM: {
//         "ColourDesc\n(Ref 6)": '011c5db3-8b84-4226-b03b-f76879b958bd',
//         'Brand': '4538ee27-dc3e-4861-a850-c99423d3bc9d',
//         "Season\n(Ref 1)": '3d3328fd-f5be-44dc-a599-e7d049d5bace',
//         Department: '869803bc-95fd-4bfb-ac22-429fe69c4358',
//         // "Fabric\n(Ref 2)": '48c313ae-38f0-482e-90d4-97d7758b840b' //tbd
//     },
//     Protex: {
//         'product search 1 description': '869803bc-95fd-4bfb-ac22-429fe69c4358',
//         'product colour season/year': '3d3328fd-f5be-44dc-a599-e7d049d5bace',
//         'Season/Year': 'c373a567-1f82-42dc-868e-43feb51951bf',
//         'product search 2 description': '358a2ea2-84d6-437f-8d58-756401a6304b',
//         'product colour search 1 description': 'ba769c13-3d8b-4478-9b10-6d12d28b4830'
//     },
//     siteAttributes: {
//         "Colour": '011c5db3-8b84-4226-b03b-f76879b958bd'
//     },
//     siteData: {
//         models_model: '7a78578d-26bd-4942-a506-a327abef94e4',
//         Models_AdditionalInformation1: 'e5ca508c-fd0e-4f18-b067-c3c32586de3a',
//         Models_AdditionalInformation2: 'f9c6722a-e781-4eb3-8c97-9be6d00934c5'
//     }
// };

// const parentMappables = {
//     FM: {
//         'retail price (inc vat)': '42263a17-a049-4c01-b60d-9a7c19d529c3',
//         'uk wholesale (gbp) (exc vat) (user 3)': '7706e970-907e-402d-aa3d-163649f913ad',
//         'eu wholesale (eur) (exc vat) (user 2)': 'edd061e8-6ca1-487e-85cb-5e00b0a5aebb',
//         "ColourDesc\n(Ref 6)": '011c5db3-8b84-4226-b03b-f76879b958bd',
//         Colour: '3d72fded-8126-4a5c-b3d2-2d3ac8496c9c',
//         'Brand': '4538ee27-dc3e-4861-a850-c99423d3bc9d',
//         "Season\n(Ref 1)": '3d3328fd-f5be-44dc-a599-e7d049d5bace',
//         Department: '869803bc-95fd-4bfb-ac22-429fe69c4358',
//     },
//     Protex: {
//         'product search 1 description': '869803bc-95fd-4bfb-ac22-429fe69c4358',
//         'product colour season/year': '3d3328fd-f5be-44dc-a599-e7d049d5bace',
//         'Season/Year': 'c373a567-1f82-42dc-868e-43feb51951bf',
//         'product search 2 description': '358a2ea2-84d6-437f-8d58-756401a6304b',
//         'product colour search 1 description': 'ba769c13-3d8b-4478-9b10-6d12d28b4830',
//         'Product Colour Code': '3d72fded-8126-4a5c-b3d2-2d3ac8496c9c'
//     },
//     siteAttributes: {
//         'pattern': 'd7684a3c-01db-4f28-9947-c125cadb1ce3',
//         'material': '0f4c88c6-1983-4ee2-937b-777c1670b51f',
//         'style': '32226684-a871-47a2-a61c-55df3a202121',
//         "Colour": '011c5db3-8b84-4226-b03b-f76879b958bd'
//     },
//     siteData: {
//         models_model: '7a78578d-26bd-4942-a506-a327abef94e4',
//         Models_AdditionalInformation1: 'e5ca508c-fd0e-4f18-b067-c3c32586de3a',
//         Models_AdditionalInformation2: 'f9c6722a-e781-4eb3-8c97-9be6d00934c5'
//     }
// };
// const parentProductAttribute = '259defac-d04b-4fa3-b97f-d57390a06169';
// const sizeAttribute = '0f0eb2bc-16c8-4d0c-bd05-1a81e75ae77a';
// const fitAttribute = '2fa2ad26-1a5b-49e1-84bd-a4e8b7d6b99c';
///////////////////////////////////////////////////////////////////////////////////






//mageeStaging mappings
///////////////////////////////////////////////////////////////////////////////////
const childMappables = {
    FM: {
        "ColourDesc\n(Ref 6)": '4c85f491-3de5-4114-af6d-c1ab7b870251',
        'Brand': '3d9fd37a-60e5-4e4c-8c02-dc961b67de98',
        "Season\n(Ref 1)": '13c4c99e-3565-4b4e-b3b0-25f6314f3955',
        Department: '41b3e9fa-c636-41dc-b70c-deadddff6a40',
        // "Fabric\n(Ref 2)": '48c313ae-38f0-482e-90d4-97d7758b840b' //tbd
    },
    Protex: {
        'product search 1 description': '41b3e9fa-c636-41dc-b70c-deadddff6a40',
        'product colour season/year': '13c4c99e-3565-4b4e-b3b0-25f6314f3955',
        'Season/Year': '6ce894e8-8b34-4f3f-9f44-7d56f6cbb1e5',
        'product search 2 description': '6b3daf6e-926d-4fb6-b548-2001ad352638',
        'product colour search 1 description': 'c4ae0e48-15b6-48c6-8bc0-a8f797be7aef'
    },
    siteAttributes: {
        "Colour": '4c85f491-3de5-4114-af6d-c1ab7b870251'
    },
    siteData: {
        models_model: 'e575c574-cb5b-46f2-810f-58dfd8effaf0',
        Models_AdditionalInformation1: '44585f6e-f8bc-419b-b193-f55ea0d1be9c',
        Models_AdditionalInformation2: 'ba13a327-6706-4919-b4ab-7f9b35c518bc'
    }
};

const parentMappables = {
    FM: {
        'retail price (inc vat)': 'a0602adc-4ab7-47b4-bb31-7ad3cba62814',
        'uk wholesale (gbp) (exc vat) (user 3)': '09f44d56-55a3-460b-ab83-37ff57005259',
        'eu wholesale (eur) (exc vat) (user 2)': 'dca6bffd-9a25-4686-b85c-b57e0a8a93f8',
        "ColourDesc\n(Ref 6)": '4c85f491-3de5-4114-af6d-c1ab7b870251',
        Colour: '9301312c-f71e-4216-a204-484f8e8f2d40',
        'Brand': '3d9fd37a-60e5-4e4c-8c02-dc961b67de98',
        "Season\n(Ref 1)": '13c4c99e-3565-4b4e-b3b0-25f6314f3955',
        Department: '41b3e9fa-c636-41dc-b70c-deadddff6a40',
    },
    Protex: {
        'product search 1 description': '41b3e9fa-c636-41dc-b70c-deadddff6a40',
        'product colour season/year': '13c4c99e-3565-4b4e-b3b0-25f6314f3955',
        'Season/Year': '6ce894e8-8b34-4f3f-9f44-7d56f6cbb1e5',
        'product search 2 description': '6b3daf6e-926d-4fb6-b548-2001ad352638',
        'product colour search 1 description': 'c4ae0e48-15b6-48c6-8bc0-a8f797be7aef',
        'Product Colour Code': '9301312c-f71e-4216-a204-484f8e8f2d40'
    },
    siteAttributes: {
        'pattern': '483f4af9-3e1d-4ceb-a82e-43710372ff3b',
        'material': 'fc020182-7081-40e7-bae2-22ed017d58fd',
        'style': '956c3d4d-8eac-4584-875c-b821e53af407',
        "Colour": '4c85f491-3de5-4114-af6d-c1ab7b870251'
    },
    siteData: {
        models_model: 'e575c574-cb5b-46f2-810f-58dfd8effaf0',
        Models_AdditionalInformation1: '44585f6e-f8bc-419b-b193-f55ea0d1be9c',
        Models_AdditionalInformation2: 'ba13a327-6706-4919-b4ab-7f9b35c518bc'
    }
};

const parentProductAttribute = '427c1dcb-9f3c-4acc-be4e-03dc3e7a8dc5';
const sizeAttribute = '588ede69-9afa-4632-970d-d51b612eb5d5';
const fitAttribute = 'aa261ded-5d0c-429f-99cd-bc5e2fd5d841';
///////////////////////////////////////////////////////////////////////////////////


// Global variables
let itemList = {};
let imageList = {};
let timeStamp = '1970-01-01T00:00';
let productData = {};
let attributeNames = {};
let attributeValues = {};
let attributeAllowedValues = {};
let itemTypes = {};
let FMSupplierLookup = {};
let suppliers = {};
let supplierTempLookup = {};


//Will loop through these to remove fit letter from size, since the data is inconsistent
const fitLookup = {
    R: "Regular",
    S: 'Short',
    L: 'Long',
    XS: 'Extra Short',
    XL: 'Extra Long'
};

function getCountryCode(countryName) {
        
    // Handle case-insensitive search
    const searchName = countryName.trim().toLowerCase();

    // Search through the countries object
    for (const code in countries) {
        if (countries[code].name.toLowerCase() === searchName) {
            return code;
        }
    }

    // If no exact match found, try partial match
    for (const code in countries) {
        if (countries[code].name.toLowerCase().includes(searchName)) {
            return code;
        }
    }

    // No match found
    return null;
}

// Load data from JSON file
async function loadDataFromFile() {
    try {
        if (fs.existsSync(itemsFilePath)) {
            const fileData = fs.readFileSync(itemsFilePath, 'utf8');
            const parsedData = JSON.parse(fileData);
            
            itemList = parsedData.items || {};
            timeStamp = parsedData.timestamp || '1970-01-01T00:00';
            
            console.log(`Data loaded. Last timestamp: ${timeStamp}`);
        } else {
            console.log('No existing data found. Starting fresh.');
            fs.mkdirSync(path.dirname(itemsFilePath), { recursive: true });
        }

        if (fs.existsSync(imagesFilePath)) {
            const fileData = fs.readFileSync(imagesFilePath, 'utf8');
            imageList = JSON.parse(fileData);
            
            
            console.log(`Data loaded. Last timestamp: ${timeStamp}`);
        } else {
            console.log('No existing data found. Starting fresh.');
            fs.mkdirSync(path.dirname(imagesFilePath), { recursive: true });
        }
    } catch (error) {
        console.log('Error loading data:', error);
        fs.mkdirSync(path.dirname(itemsFilePath), { recursive: true });
    }
}

// Save data to JSON file
async function saveDataToFile() {
    try {
        fs.writeFileSync(itemsFilePath, JSON.stringify({
            timestamp: timeStamp,
            items: itemList
        }, null, 2));
        fs.writeFileSync(imagesFilePath, JSON.stringify(imageList, null, 2));
    } catch (error) {
        console.error('Error saving data:', error);
    }
}

// Fetch items from API
async function getItems() {
    const now = new Date();
    let timeStart = now.toISOString().substring(0, 16);
    
    await common.loopThrough(
        'Getting Items', 
        `https://${global.enviroment}/v0/items`, 
        'size=1000&sortDirection=ASC&sortField=timeUpdated', 
        `[status]!={1}%26%26[timeUpdated]>>{${timeStamp}}`, 
        async (item) => {
            itemList[item.sku.toLowerCase().trim().replaceAll(' ', '')] = item.itemId
        }
    );
    
    timeStamp = timeStart;
    await saveDataToFile();
}

async function getItemTypes(){
    await common.loopThrough('Getting Types', `https://${global.enviroment}/v0/item-types`, 'size=1000', '[status]!={1}', (type)=>{
        itemTypes[type.name.toLowerCase().trim()] = type.itemTypeId
    })
};

async function getSuppliers(){
    await common.loopThrough('Getting Suppliers', `https://${global.enviroment}/v1/suppliers`, 'size=1000', '[status]!={1}', (supp)=>{
        suppliers[supp.accountReference.toLowerCase().trim()] = supp.supplierId
        supplierTempLookup[supp.name.toLowerCase().trim()] = supp.accountReference
    })
};

async function getAttributes(){
    await common.loopThrough('Getting Attributes', `https://${global.enviroment}/v0/item-attributes`, 'size=1000', '[status]!={1}', (attribute)=>{
        attributeAllowedValues[attribute.itemAttributeId] = attribute
    })
};


// Process Protex SKU CSV
async function processProtexCSV() {
    // Get all CSV files in the folder
    const files = await fs.promises.readdir(protexSKUFolder);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} CSV files in ${protexSKUFolder}`);
    
    // Process each CSV file sequentially
    for (const csvFile of csvFiles) {
        const protexFilePath = path.join(protexSKUFolder, csvFile);
        console.log(`Processing ${csvFile}...`);
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(protexFilePath)
                .pipe(fastcsv.parse({ headers: true, trim: true }))
                .on('error', error => {
                    console.error('Error parsing CSV:', error);
                    reject(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    const productCode = `${row['Product']}_${row['Colour Code']}`.trim();
                    const sku = row['SKU'].trim();
                    const fitName = row['Fit Name'] ? row['Fit Name'].trim() : '';
                    const sizeName = row['Size Name'] ? row['Size Name'].toString().trim() : '';
                    const productName = row['Product Name'] ? row['Product Name'].trim() : '';
                    const ean = row['EAN'] ? row['EAN'].toString().trim() : '';
                    
                    // Initialize product if needed
                    if (!productData[productCode]) {
                        productData[productCode] = {
                            name: productName,
                            attributes: {},
                            variants: {},
                            singleItemSKU: row['Product']
                        };
                    }

                    // Create the properly formatted variant key (ProductCode_ColorCode-SizeFit)
                    const formattedVariantKey = `${row['Product']}_${row['Colour Code']}-${row['Size'] || ''}${row['Fit'] ? row['Fit'].charAt(0) : ''}`;
                    
                    // Use the formatted variant key as the primary key
                    if (!productData[productCode].variants[formattedVariantKey]) {
                        productData[productCode].variants[formattedVariantKey] = {
                            name: `${productName}_${row['Colour Code']}-${sizeName}${fitName ? fitName.charAt(0) : ''}`,
                            barcodes: [],
                            attributes: {},
                            altCodes: [],
                            sizeValue: row['Size'],
                            fitValue: fitLookup[row['Fit']]
                        };
                    }
                    
                    // Add data
                    if (ean) productData[productCode].variants[formattedVariantKey].barcodes.push(ean);
                    if (sku && sku !== formattedVariantKey) productData[productCode].variants[formattedVariantKey].altCodes.push(sku);
                    
                    // Add alt SKU format (with hyphen)
                    const altSku = `${row['Product']}-${row['Colour Code']}-${row['Fit'] || ''}-${row['Size'] || ''}`.replace(/--+/g, '-').replace(/-$/,'');
                    if (altSku && altSku !== formattedVariantKey && !productData[productCode].variants[formattedVariantKey].altCodes.includes(altSku)) {
                        productData[productCode].variants[formattedVariantKey].altCodes.push(altSku);
                    }
                    
                    // Add dash-separated version of the formatted variant key as an alt code
                    const dashVariantKey = `${row['Product']}-${row['Colour Code']}-${row['Size'] || ''}${row['Fit'] ? row['Fit'].charAt(0) : ''}`;
                    if (dashVariantKey && dashVariantKey !== formattedVariantKey && !productData[productCode].variants[formattedVariantKey].altCodes.includes(dashVariantKey)) {
                        productData[productCode].variants[formattedVariantKey].altCodes.push(dashVariantKey);
                    }
                    
                    // Add the SKU from Protex as protexSKU
                    if (sku) {
                        productData[productCode].variants[formattedVariantKey].protexSKU = sku;
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    resolve();
                });
        });
    }
    
    console.log(`Processed all CSV files. Total products: ${Object.keys(productData).length}`);
}

// Process FM CSV
async function processFmCSV() {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(fmFilePath)
            .pipe(fastcsv.parse({ 
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
                
                // Map FM fields to Protex fields
                const product = row['SPC'] ? row['SPC'].trim() : '';
                const colorCode = row['Colour'] ? (isNaN(Number(row['Colour'].trim())) ? row['Colour'].trim() : String(Number(row['Colour'].trim()))) : '';
                const size = row['Size'] ? row['Size'].trim() : '';
                const description = row['Description'] ? row['Description'].trim() : '';
                
                // Skip if essential data is missing
                if (!product || !colorCode) {
                    stream.resume();
                    return;
                }
                
                const productCode = `${product}_${colorCode}`;

                
                // Create or update product
                if (!productData[productCode]) {
                    productData[productCode] = {
                        name: description || product,
                        attributes: {
                            Protex: {}
                        },
                        variants: {},
                        singleItemSKU: product
                    };
                } else if (!productData[productCode].attributes.Protex) {
                    // Ensure Protex attribute exists if the product already exists
                    productData[productCode].attributes.Protex = {};
                }
                
                // Try to determine fit and size from the combined size field
                let sizeName = size;
                let fitChar = '';
                
                // Check if the size ends with a letter that might indicate fit (S, R, L)
                const sizeMatch = size.match(/^(\d+)([SRL])$/i);
                if (sizeMatch) {
                    sizeName = sizeMatch[1];
                    fitChar = sizeMatch[2].toUpperCase();
                }
                
                // Create variant key
                const formattedVariantKey = `${product}_${colorCode}-${sizeName}${fitChar}`;
                
                // Create or update variant
                if (!productData[productCode].variants[formattedVariantKey]) {
                    productData[productCode].variants[formattedVariantKey] = {
                        name: `${description || product}_${colorCode}-${size}`,
                        barcodes: [],
                        attributes: {},
                        altCodes: []
                    };
                }

                productData[productCode].variants[formattedVariantKey].attributes.FM = row;
                productData[productCode].attributes.FM = row;

                if (productData[productCode].variants[formattedVariantKey].fitValue === undefined) {
                    productData[productCode].variants[formattedVariantKey].fitValue = fitLookup[row['Length\n(Ref 13)'].trim()]
                }
                
                // Only set the sizeValue if it's not already defined
                if (productData[productCode].variants[formattedVariantKey].sizeValue === undefined) {
                    let sizeValue = row['Size'] ? row['Size'].trim() : '';
                    
                    // If the Length (Ref 13) field exists, remove it from the end of the size
                    if (row['Length\n(Ref 13)'] && sizeValue.endsWith(row['Length\n(Ref 13)'])) {
                        sizeValue = sizeValue.substring(0, sizeValue.length - row['Length\n(Ref 13)'].length).trim();
                    }
                    
                    // Set the processed size value on the variant
                    productData[productCode].variants[formattedVariantKey].sizeValue = sizeValue;
                }
                
                // Add barcodes
                const systemBarcode = row['System Barcode'] ? row['System Barcode'].toString().trim() : '';
                const gtin1 = row['GTIN Barcode (H1)'] ? row['GTIN Barcode (H1)'].toString().trim() : '';
                const gtin2 = row['GTIN Barcode (1)'] ? row['GTIN Barcode (1)'].toString().trim() : '';
                
                if (systemBarcode && !productData[productCode].variants[formattedVariantKey].barcodes.includes(systemBarcode)) {
                    productData[productCode].variants[formattedVariantKey].barcodes.push(systemBarcode);
                }
                
                if (gtin1 && !isNaN(gtin1) && !productData[productCode].variants[formattedVariantKey].barcodes.includes(gtin1)) {
                    productData[productCode].variants[formattedVariantKey].barcodes.push(gtin1);
                }
                
                if (gtin2 && !isNaN(gtin2) && !productData[productCode].variants[formattedVariantKey].barcodes.includes(gtin2)) {
                    productData[productCode].variants[formattedVariantKey].barcodes.push(gtin2);
                }
                
                // Add alternate SKU codes
                const dashVariantKey = `${product}-${colorCode}-${sizeName}${fitChar}`;
                if (dashVariantKey && !productData[productCode].variants[formattedVariantKey].altCodes.includes(dashVariantKey)) {
                    productData[productCode].variants[formattedVariantKey].altCodes.push(dashVariantKey);
                }
                
                stream.resume();
            })
            .on('end', () => {
                console.log(`Processed FM CSV data`);
                resolve();
            });
    });
}

// Process Protex Product CSV files from a folder
async function processProtexProductCSV() {
    // Get all CSV files in the folder
    const files = await fs.promises.readdir(protexProductFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} Protex Product CSV files in ${protexProductFolderPath}`);
    
    // Process each CSV file sequentially
    for (const csvFile of csvFiles) {
        const protexProductFilePath = path.join(protexProductFolderPath, csvFile);
        console.log(`Processing Protex Product CSV: ${csvFile}...`);
        
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(protexProductFilePath)
                .pipe(fastcsv.parse({ headers: true, trim: true }))
                .on('error', error => {
                    console.error(`Error parsing Protex Product CSV ${csvFile}:`, error);
                    reject(error);
                })
                .on('data', row => {
                    stream.pause();
                    
                    const productCode = row['Product Code'] ? row['Product Code'].trim() : '';
                    const productColorCode = row['Product Colour Code'] ? row['Product Colour Code'].trim() : '';
                    
                    // Skip if essential data is missing
                    if (!productCode || !productColorCode) {
                        stream.resume();
                        return;
                    }
                    
                    const combinedProductCode = `${productCode}_${productColorCode}`;
                    
                    // Check if the product already exists in our data
                    if (productData[combinedProductCode]) {
                        // Create a 'Protex' object for the product attributes
                        productData[combinedProductCode].attributes['Protex'] = row;
                        
                        // Also add the Protex data to all variants of this product
                        Object.keys(productData[combinedProductCode].variants).forEach(variantKey => {
                            productData[combinedProductCode].variants[variantKey].attributes['Protex'] = row
                        });
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    resolve();
                });
        });
    }
    
    console.log(`Processed all Protex Product CSV files`);
}

// Process Site Products CSV
async function processSiteProductsCSV() {
    return new Promise((resolve, reject) => {
        // Keep track of unmatched products for reporting
        const unmatchedProducts = [];
        let matchCount = 0;
        
        // Create a barcode index for faster lookups
        console.log('Building barcode index for faster site product matching...');
        const barcodeIndex = {};
        for (const productCode in productData) {
            const product = productData[productCode];
            
            for (const variantKey in product.variants) {
                const variant = product.variants[variantKey];
                
                if (variant.barcodes && variant.barcodes.length > 0) {
                    variant.barcodes.forEach(barcode => {
                        const numericBarcode = parseInt(barcode, 10);
                        if (!isNaN(numericBarcode)) {
                            if (!barcodeIndex[numericBarcode]) {
                                barcodeIndex[numericBarcode] = [];
                            }
                            barcodeIndex[numericBarcode].push({ product, variant });
                        }
                    });
                }
            }
        }
        console.log(`Built index with ${Object.keys(barcodeIndex).length} unique barcodes`);
        
        const stream = fs.createReadStream(siteProductsFilePath)
            .pipe(fastcsv.parse({ headers: true, trim: true }))
            .on('error', error => {
                console.error('Error parsing Site Products CSV:', error);
                reject(error);
            })
            .on('data', row => {
                stream.pause();
                
                // Extract the needed fields
                const externalStockID = row['Stock_ExternalStockID'] ? parseInt(row['Stock_ExternalStockID'].toString().trim(), 10) : null;
                const modelExternal = row['Models_ModelExternal'] ? row['Models_ModelExternal'].toString().trim() : '';
                const stockOption = row['Stock_Option'] ? row['Stock_Option'].toString().trim() : '';
                
                let matched = false;
                
                // First attempt: Use barcode index for O(1) lookups
                if (externalStockID && barcodeIndex[externalStockID]) {
                    const matches = barcodeIndex[externalStockID];
                    matches.forEach(({ product, variant }) => {
                        // Found a match by barcode
                        if (!variant.attributes.siteData) {
                            variant.attributes.siteData = row;
                        }
                        
                        // Also add to the parent product
                        if (!product.attributes.siteData) {
                            product.attributes.siteData = row;
                        }
                    });
                    
                    matched = true;
                    matchCount++;
                }
                
                // Second attempt: Try to match by constructing a key from ModelExternal and StockOption
                if (!matched && modelExternal && stockOption) {
                    // Parse modelExternal to remove leading zeros in the color code
                    let parts = modelExternal.split('_');
                    if (parts.length === 2) {
                        const productCode = parts[0];
                        const colorCode = parts[1].replace(/^0+/, ''); // Remove leading zeros
                        
                        // Construct a potential variant key
                        const constructedProductCode = `${productCode}_${colorCode}`;
                        const constructedVariantKey = `${productCode}_${colorCode}-${stockOption}`;
                        
                        // Check if this product and variant exist
                        if (productData[constructedProductCode] && 
                            productData[constructedProductCode].variants[constructedVariantKey]) {
                            
                            // Found a match by constructed key
                            const product = productData[constructedProductCode];
                            const variant = product.variants[constructedVariantKey];
                            
                            if (!variant.attributes.siteData) {
                                variant.attributes.siteData = row;
                            }
                            
                            if (!product.attributes.siteData) {
                                product.attributes.siteData = row;
                            }
                            
                            matched = true;
                            matchCount++;
                        }
                    }
                }
                
                // If no match was found, add to unmatched products
                if (!matched) {
                    unmatchedProducts.push({
                        externalStockID,
                        modelExternal,
                        stockOption,
                        fullRow: row
                    });
                }
                
                stream.resume();
            })
            .on('end', () => {
                console.log(`Processed Site Products CSV data: ${matchCount} matches found`);
                console.log(`Unmatched products: ${unmatchedProducts.length}`);
                
                // Log first few unmatched products for debugging
                if (unmatchedProducts.length > 0) {
                    console.log('Sample of unmatched products:');
                    console.log(unmatchedProducts.slice(0, 5));
                }
                
                resolve();
            });
    });
}

// Process Attribute Names CSV
async function processAttributeNamesCSV() {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(attributeNamesFilePath)
            .pipe(fastcsv.parse({ headers: true, trim: true }))
            .on('error', error => {
                console.error('Error parsing Attribute Names CSV:', error);
                reject(error);
            })
            .on('data', row => {
                const attributeID = parseInt(row['AttributeID'], 10);
                const attributeName = row['AttributeName'] ? row['AttributeName'].trim() : '';
                
                if (attributeID && attributeName) {
                    attributeNames[attributeID] = attributeName;
                }
            })
            .on('end', () => {
                console.log(`Processed Attribute Names CSV: ${Object.keys(attributeNames).length} attributes loaded`);
                resolve();
            });
    });
}

async function getFMSupplierLookup() {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(FMSupplierLookupCSV)
            .pipe(fastcsv.parse({ headers: true, trim: true }))
            .on('error', error => {
                console.error('Error parsing Attribute Names CSV:', error);
                reject(error);
            })
            .on('data', row => {
                FMSupplierLookup[row['Supplier'].toLowerCase().trim()] = row['supplier code'].toLowerCase().trim()
            })
            .on('end', () => {
                resolve();
            });
    });
}

// Process Attribute Values CSV
async function processAttributeValuesCSV() {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(attributeValuesFilePath)
            .pipe(fastcsv.parse({ headers: true, trim: true }))
            .on('error', error => {
                console.error('Error parsing Attribute Values CSV:', error);
                reject(error);
            })
            .on('data', row => {
                const attributeValueID = parseInt(row['AttributeValueID'], 10);
                const value = row['Value'] ? row['Value'].trim() : '';
                
                if (attributeValueID && value) {
                    attributeValues[attributeValueID] = value;
                }
            })
            .on('end', () => {
                console.log(`Processed Attribute Values CSV: ${Object.keys(attributeValues).length} values loaded`);
                resolve();
            });
    });
}

async function processImagesCSV() {
    return new Promise((resolve, reject) => {
        let stockIDLookup = {};
        let processedCount = 0;
        let totalCount = 0;
        
        // Create a lookup table for faster matching
        console.log('Building model lookup table for image processing...');
        for (const parent of Object.keys(productData)) {
            // Initialize images array for each product
            productData[parent].images = [];
            
            for (const child of Object.keys(productData[parent].variants)) {
                const variant = productData[parent].variants[child];
                
                // Add to lookup if ModelID exists
                if (variant.attributes?.siteData?.['Models_ModelID']) {
                    const modelId = variant.attributes.siteData['Models_ModelID'];
                    stockIDLookup[modelId] = parent;
                }
            }
        }
        
        console.log(`Built model lookup with ${Object.keys(stockIDLookup).length} unique model IDs`);

        const stream = fs.createReadStream(modelImagesFilePath)
            .pipe(fastcsv.parse({ headers: true, trim: true }))
            .on('error', error => {
                console.error('Error parsing Model Images CSV:', error);
                reject(error);
            })
            .on('data', async row => {
                stream.pause();

                totalCount++;
            
                // Make sure we're accessing the ModelID field correctly
                const modelId = row['Models_ModelID'];
                
                if (modelId && stockIDLookup[modelId] && row['ImageURLs']) {
                    const parent = stockIDLookup[modelId];
                    const imageUrls = row['ImageURLs'].split(',');
                    
                    // Process each image URL for this model
                    for (const imageUrl of imageUrls) {
                        if (!imageList[imageUrl]){
                            let newURL = await common.postImage(imageUrl).then(r=>{return r.data.location})
                            imageList[imageUrl] - newURL
                        }
                        if(!productData[parent].images.includes(imageList[imageUrl])){productData[parent].images.push(imageList[imageUrl])}
                    }
                }

                
                stream.resume();
            })
            .on('end', async () => {
                try {
                    resolve();
                } catch (error) {
                    console.error('Error during image processing:', error);
                    reject(error);
                }
            });
    });
}

// Process Stock Attributes CSV
async function processStockAttributesCSV() {
    return new Promise(async (resolve, reject) => {
        console.log('Building Stock_StockID index for faster attribute processing...');
        
        let stockIDLookup = {};
        
        // Create a lookup table for faster matching
        let variantCount = 0;
        
        // Initialize site attributes for all variants
        for (const parent of Object.keys(productData)) {
            for (const child of Object.keys(productData[parent].variants)) {
                const variant = productData[parent].variants[child];
                
                // Initialize site attributes container
                if (!variant.attributes.siteAttributes) {
                    variant.attributes.siteAttributes = {};
                }
                
                // Add to lookup if StockID exists
                if (variant.attributes?.siteData?.['Stock_StockID']) {
                    const stockId = variant.attributes.siteData['Stock_StockID'];
                    stockIDLookup[stockId] = { parent, child };
                    variantCount++;
                }
            }
        }
        
        console.log(`Built index with ${Object.keys(stockIDLookup).length} unique Stock_StockIDs from ${variantCount} variants`);
        
        let matchCount = 0;
        let noMatchCount = 0;
        
        const stream = fs.createReadStream(stockAttributesFilePath)
            .pipe(fastcsv.parse({ headers: true, trim: true }))
            .on('error', error => {
                console.error('Error parsing Stock Attributes CSV:', error);
                reject(error);
            })
            .on('data', row => {
                stream.pause();
                
                const stockID = row['StockID'];
                const attributeID = parseInt(row['AttributeID'], 10);
                const attributeValueID = parseInt(row['AttributeValueID'], 10);
                
                // Skip if no attribute name or value is found
                if (!attributeNames[attributeID] || !attributeValues[attributeValueID]) {
                    noMatchCount++;
                    stream.resume();
                    return;
                }
                
                // Use lookup table for O(1) access instead of nested loops
                if (stockIDLookup[stockID]) {
                    const { parent, child } = stockIDLookup[stockID];
                    const attributeName = attributeNames[attributeID];
                    const attributeValue = attributeValues[attributeValueID];
                    
                    // Add to variant
                    productData[parent].variants[child].attributes.siteAttributes[attributeName] = attributeValue;
                    
                    // Also add to parent if needed
                    if (!productData[parent].attributes.siteAttributes) {
                        productData[parent].attributes.siteAttributes = {};
                    }
                    productData[parent].attributes.siteAttributes[attributeName] = attributeValue;
                    
                    matchCount++;
                } else {
                    noMatchCount++;
                }
                
                stream.resume();
            })
            .on('end', () => {
                console.log(`Processed Stock Attributes CSV: ${matchCount} matches found, ${noMatchCount} not matched`);
                resolve();
            });
    });
}

function collectAttributeValues() {
    console.log('Collecting all unique attribute values...');
    
    // Create a results object to store all values by attribute ID
    const results = {
        sizeValues: new Set(), // Special case for sizeValues
        fitValues: new Set()
    };
    
    // Initialize sets for each attribute ID in the mappings
    // Process parent mappings
    for (const source of Object.keys(parentMappables)) {
        for (const [field, attributeId] of Object.entries(parentMappables[source])) {
            if (!results[attributeId]) {
                results[attributeId] = new Set();
            }
        }
    }
    
    // Process variant mappings 
    for (const source of Object.keys(childMappables)) {
        for (const [field, attributeId] of Object.entries(childMappables[source])) {
            if (!results[attributeId]) {
                results[attributeId] = new Set();
            }
        }
    }
    
    // Process all products and collect values
    for (const parent of Object.keys(productData)) {
        const parentProduct = productData[parent];
        
        // Process parent attributes
        for (const source of Object.keys(parentMappables)) {
            if (parentProduct.attributes && parentProduct.attributes[source]) {
                const sourceData = parentProduct.attributes[source];
                
                // Create a case-insensitive lookup map of the source data
                const caseInsensitiveData = {};
                for (const key in sourceData) {
                    caseInsensitiveData[key.toLowerCase()] = key;
                }
                
                // Collect values for each attribute ID
                for (const [field, attributeId] of Object.entries(parentMappables[source])) {
                    // Try to find the field case-insensitively
                    const actualFieldKey = caseInsensitiveData[field.toLowerCase()] || field;
                    
                    // Check if the field exists in the source data
                    if (sourceData[actualFieldKey] !== undefined && sourceData[actualFieldKey] !== null && sourceData[actualFieldKey] !== '') {
                        results[attributeId].add(sourceData[actualFieldKey].toString().trim());
                    }
                }
            }
        }
        
        // Process all variants
        for (const child of Object.keys(parentProduct.variants)) {
            const variant = parentProduct.variants[child];
            
            // Collect sizeValues
            if (variant.sizeValue !== undefined && variant.sizeValue !== '') {
                results.sizeValues.add(variant.sizeValue.toString().trim());
            }

            // Collect sizeValues
            if (variant.fitValue !== undefined && variant.fitValue !== '') {
                results.fitValues.add(variant.fitValue.toString().trim());
            }
            
            // Process variant attributes
            for (const source of Object.keys(childMappables)) {
                if (variant.attributes && variant.attributes[source]) {
                    const sourceData = variant.attributes[source];
                    
                    // Create a case-insensitive lookup map of the source data
                    const caseInsensitiveData = {};
                    for (const key in sourceData) {
                        caseInsensitiveData[key.toLowerCase()] = key;
                    }
                    
                    // Collect values for each attribute ID
                    for (const [field, attributeId] of Object.entries(childMappables[source])) {
                        // Try to find the field case-insensitively
                        const actualFieldKey = caseInsensitiveData[field.toLowerCase()] || field;
                        
                        // Check if the field exists in the source data
                        if (sourceData[actualFieldKey] !== undefined && sourceData[actualFieldKey] !== null && sourceData[actualFieldKey] !== '') {
                            results[attributeId].add(sourceData[actualFieldKey].toString().trim());
                        }
                    }
                }
            }
        }
    }
    
    // Convert Sets to Arrays for the final result
    const finalResults = {};
    for (const attributeId of Object.keys(results)) {
        finalResults[attributeId] = Array.from(results[attributeId]);
    }
    
    console.log(`Collected values for ${Object.keys(finalResults).length} attribute IDs`);
    return finalResults;
}

async function makeItems() {
    let failedList = []
    let allValues = collectAttributeValues();
    
    // Update attribute allowed values without error handling
    for (const attribute of Object.keys(allValues)) {
        let remoteValue = attribute
        if(attribute == 'sizeValues'){remoteValue = sizeAttribute}
        if(attribute == 'fitValues'){
            remoteValue = fitAttribute
        }
        if (attributeAllowedValues[remoteValue].type == 4 || attributeAllowedValues[remoteValue].type == 6) {
            for (const value of allValues[attribute]){
                if(!attributeAllowedValues[remoteValue].allowedValues.includes(value)){attributeAllowedValues[remoteValue].allowedValues.push(value)}
            }
            attributeAllowedValues[remoteValue].allowedValues = allValues[attribute];
            await common.requester('patch', `https://${global.enviroment}/v0/item-attributes/${remoteValue}`, attributeAllowedValues[remoteValue]);
        }
    }

    console.log('Creating items from product data...');
    const createdItems = [];

    for (const parent of Object.keys(productData)) {
        await processItem(parent)
    }

    fs.writeFileSync('./failed.txt', failedList.join('\n'), 'utf8');



    async function processItem(parent){
        try{
            console.log(`Processing parent product: ${parent}`);
            const parentProduct = productData[parent];


            const type = parentProduct?.attributes?.FM?.['Type'];
            if (!type || itemTypes?.[type.toLowerCase().trim()] == undefined){
                await common.requester('post', `https://${global.enviroment}/v0/item-types`, {"name":parentProduct.attributes.FM['Type']}).then(r=>{
                    itemTypes[parentProduct.attributes.FM['Type'].toLowerCase().trim()] = r.data.data.id
                })
            }

            // Check if sizeValue and fitValue vary across children
            const sizeValues = new Set();
            const fitValues = new Set();
            
            // Collect all distinct size and fit values
            for (const child of Object.keys(parentProduct.variants)) {
                const variant = parentProduct.variants[child];
                
                if (variant.sizeValue !== undefined && variant.sizeValue !== '') {
                    sizeValues.add(variant.sizeValue);
                }
                
                if (variant.fitValue !== undefined && variant.fitValue !== '') {
                    fitValues.add(variant.fitValue);
                }
            }

            
            // Check if we have variation (more than one distinct value)
            const hasSizeValueVariation = sizeValues.size > 1;
            const hasFitValueVariation = fitValues.size > 1;
            
            // Set flags based on variation, not uniqueness
            parentProduct.hasSizeValueUniqueness = hasSizeValueVariation;
            parentProduct.hasFitValueUniqueness = hasFitValueVariation;
            
            // Generate parent product attributes
            const parentAttributes = [{
                "itemAttributeId": parentProductAttribute,
                "value": parent.split('_')[0]
            }];
            
            // Process each data source using the parentMappables
            for (const source of Object.keys(parentMappables)) {
                if (parentProduct.attributes && parentProduct.attributes[source]) {
                    const sourceData = parentProduct.attributes[source];
                    
                    // Create a case-insensitive lookup map of the source data
                    const caseInsensitiveData = {};
                    for (const key in sourceData) {
                        caseInsensitiveData[key.toLowerCase()] = key;
                    }
                    
                    // Loop through the field mappings for this source
                    for (const [field, attributeId] of Object.entries(parentMappables[source])) {
                        // Try to find the field case-insensitively
                        const actualFieldKey = caseInsensitiveData[field.toLowerCase()] || field;
                        
                        // Check if the field exists in the source data
                        if (sourceData[actualFieldKey] !== undefined && sourceData[actualFieldKey] !== null && sourceData[actualFieldKey] !== '') {
                            parentAttributes.push({
                                "itemAttributeId": attributeId,
                                "value": sourceData[actualFieldKey].toString().trim()
                            });
                        }
                    }
                }
            }
            
            let parentData = {
                "format": 2,
                "name": parentProduct.name,
                "sku": parent,
                "attributes": parentAttributes,
                description: parentProduct?.attributes?.siteData?.['Models_Description'],
                typeId: itemTypes[parentProduct.attributes.FM['Type'].toLowerCase().trim()],
                "images": (()=>{
                    let returnArr = []
                    for (const image of (parentProduct.images || [])){
                        returnArr.push({
                            "uri":image
                        })
                    }
                    return returnArr
                })(),
                variableAttributes: [],
                variantItems: [],
                appendAttributes: true
            };
    

            if (parentProduct.hasSizeValueUniqueness){
                parentData.variableAttributes.push(sizeAttribute)
            }
            if (parentProduct.hasFitValueUniqueness){
                parentData.variableAttributes.push(fitAttribute)
            }
    
            
            createdItems.push(parentData);
            
            let singleProduct = Object.keys(parentProduct.variants).length <= 1;
    
            // Now process the variants (child products) as before
            for (const child of Object.keys(parentProduct.variants)) {
                console.log(`Processing variant: ${child}`);
                const variant = parentProduct.variants[child];
                
                // Initialize attributes array - link to parent
                const attributes = [{
                    "itemAttributeId": parentProductAttribute,
                    "value": parent
                }];
    
                if (parentProduct.variants[child]?.sizeValue != undefined && parentProduct.variants[child].sizeValue != ''){
                    attributes.push({
                        "itemAttributeId": sizeAttribute,
                        "value": parentProduct.variants[child].sizeValue
                    });
                }
    
                if (parentProduct.variants[child]?.fitValue != undefined && parentProduct.variants[child].fitValue != ''){
                    attributes.push({
                        "itemAttributeId": fitAttribute,
                        "value": parentProduct.variants[child].fitValue
                    });
                }
                
                // Process each data source using the original mappables for variants
                for (const source of Object.keys(childMappables)) {
                    if (variant.attributes && variant.attributes[source]) {
                        const sourceData = variant.attributes[source];
                        
                        // Create a case-insensitive lookup map of the source data
                        const caseInsensitiveData = {};
                        for (const key in sourceData) {
                            caseInsensitiveData[key.toLowerCase()] = key;
                        }
                        
                        // Loop through the field mappings for this source
                        for (const [field, attributeId] of Object.entries(childMappables[source])) {
                            // Try to find the field case-insensitively
                            const actualFieldKey = caseInsensitiveData[field.toLowerCase()] || field;
                            
                            // Check if the field exists in the source data
                            if (sourceData[actualFieldKey] !== undefined && sourceData[actualFieldKey] !== null && sourceData[actualFieldKey] !== '') {
                                attributes.push({
                                    "itemAttributeId": attributeId,
                                    "value": sourceData[actualFieldKey].toString().trim()
                                });
                            }
                        }
                    }
                }
                
                if (itemTypes[variant.attributes.FM['Type'].toLowerCase().trim()] == undefined){
                    await common.requester('post', `https://${global.enviroment}/v0/item-types`, {"name":variant.attributes.FM['Type']}).then(r=>{
                        itemTypes[variant.attributes.FM['Type'].toLowerCase().trim()] = r.data.data.id
                    })
                }

                // Create the child item data
                let childData = {
                    "format": 0,
                    "name": Object.keys(parentProduct.variants).length > 1 ? variant.name : parentProduct.name,
                    "sku": Object.keys(parentProduct.variants).length > 1 ? child : parentProduct.singleItemSKU,
                    "barcodes": variant.barcodes || [],
                    "attributes": attributes,
                    typeId: itemTypes[variant.attributes.FM['Type'].toLowerCase().trim()],
                    appendAttributes: true,
                    harmonyCode: variant?.attributes?.FM?.['Intrastat'],
                    countryOfOrigin: getCountryCode(variant?.attributes?.FM?.["COO\n(Ref 7)"])
                };
                try{childData.salePrice = {amount: parseFloat(((variant.attributes.FM['Retail Price\n(Inc Vat)'] / (1 + (parseFloat(variant.attributes.FM['Vat Rate']) / 100)))).toFixed(5)), currency: "GBP"}}catch{}

                try {
                    if(suppliers[FMSupplierLookup[variant?.attributes?.FM?.['Supplier'].toLowerCase().trim()]] != undefined && variant?.attributes?.FM?.['Cost'] != undefined){
                        childData.unitsOfMeasure = [
                            {
                                "supplierId": suppliers[FMSupplierLookup[variant?.attributes?.FM?.['Supplier'].toLowerCase().trim()]],
                                "supplierSku": Object.keys(parentProduct.variants).length > 1 ? child : parentProduct.singleItemSKU,
                                "cost": {
                                    "amount": variant?.attributes?.FM?.['Cost'],
                                    "currency": "GBP"
                                },
                                "currency": "GBP",
                                "quantityInUnit": 1
                            }
                        ]
                    }
                } catch {

                }
                let childId = await processChildItem(childData, variant, parent);
                parentData.variantItems.push(childId);
                console.log(`Created child item: ${child}`);
            }
            
            if (singleProduct) {
                return;
            }
            
            await processParentItem(parentData, parentProduct);
        } catch (e) {
            failedList.push(parent)
            console.log(e)
        }
    }
}

async function processChildItem(childData, variant, parent){

    let oldSku = [parent.replace('_', '-'), variant.fitValue, variant.sizeValue + variant.fitValue].filter(item => item !== undefined && item !== '').join('-');

    if(itemList[oldSku.toLowerCase().trim()] && itemList[childData.sku.toLowerCase().trim()]){
        await common.requester('delete', `https://${global.enviroment}/v0/items/${itemList[childData.sku.toLowerCase().trim()]}`).then(async (r) =>{
            do{
                var status = await common.requester('get', `https://${global.enviroment}/v0/items/${itemList[childData.sku.toLowerCase().trim()]}`).then(r=>{return r.data.data.status})
                await common.sleep(500)
            } while (status != 1)
            await common.sleep(500)
        })
        await common.requester('patch', `https://${global.enviroment}/v0/items/${itemList[oldSku.toLowerCase().trim()]}`, childData)
        return itemList[oldSku.toLowerCase().trim()]
    }

    if(itemList[oldSku.toLowerCase().trim()]){
        await common.requester('patch', `https://${global.enviroment}/v0/items/${itemList[oldSku.toLowerCase().trim()]}`, childData)
        return itemList[oldSku.toLowerCase.trim()]
    }



    if(itemList[childData.sku.toLowerCase().trim()]){
        await common.requester('patch', `https://${global.enviroment}/v0/items/${itemList[childData.sku.toLowerCase().trim()]}`, childData)
        return itemList[childData.sku.toLowerCase().trim()]
    }
    for (const i of variant.altCodes){
        if(itemList[i.toLowerCase().trim()]){
            await common.requester('patch', `https://${global.enviroment}/v0/items/${itemList[i.toLowerCase().trim()]}`, childData)
            return itemList[i.toLowerCase().trim()]
        }
    }
    return common.requester('post', `https://${global.enviroment}/v0/items`, childData).then(r=>{
        return r.data.data.id
    })
}

async function processParentItem(parentData, parentProduct){
    if(itemList[parentData.sku.toLowerCase().trim()]){
        await common.requester('patch', `https://${global.enviroment}/v0/variable-items/${itemList[parentData.sku.toLowerCase().trim()]}`, parentData)
        return itemList[parentData.sku.toLowerCase().trim()]
    }

    for (const i of parentProduct?.altCodes || []){
        if(itemList[i.toLowerCase().trim()]){
            await common.requester('patch', `https://${global.enviroment}/v0/variable-items/${itemList[i.toLowerCase().trim()]}`, parentData).then(r=>{
                return r.data.data.id
            })
        }
    }


    return common.requester('post', `https://${global.enviroment}/v0/variable-items`, parentData).then(r=>{return r.data.id})
}

// Main execution function
async function run() {
    console.time('Total Processing Time');
    
    // try {
        console.time('Load Data');
        await loadDataFromFile();
        console.timeEnd('Load Data');
        
        await getAttributes()

        await getItemTypes()

        await getSuppliers()

        await getFMSupplierLookup()

        console.time('Get Items');
        await getItems();
        console.timeEnd('Get Items');
        
        console.time('Process Protex CSV');
        await processProtexCSV();
        console.timeEnd('Process Protex CSV');
        
        console.time('Process FM CSV');
        await processFmCSV();
        console.timeEnd('Process FM CSV');
        
        console.time('Process Protex Product CSV');
        await processProtexProductCSV();
        console.timeEnd('Process Protex Product CSV');
        
        console.time('Process Site Products CSV');
        await processSiteProductsCSV();
        console.timeEnd('Process Site Products CSV');
        
        // Process attributes in sequence
        console.time('Process Attribute Names CSV');
        await processAttributeNamesCSV();
        console.timeEnd('Process Attribute Names CSV');
        
        console.time('Process Attribute Values CSV');
        await processAttributeValuesCSV();
        console.timeEnd('Process Attribute Values CSV');
        
        console.time('Process Stock Attributes CSV');
        await processStockAttributesCSV();
        console.timeEnd('Process Stock Attributes CSV');

        console.time('Process Images CSV');
        await processImagesCSV();
        console.timeEnd('Process Images CSV');
        
        console.time('Save Product Data');
        await saveDataToFile();
        console.timeEnd('Save Product Data');

        console.time('Make Items');
        await makeItems();
        console.timeEnd('Make Items');
        
        console.log('Product Data processing completed successfully');
    // } catch (error) {
    //     console.error('Error during data processing:', error);
    // } finally {
    //     console.timeEnd('Total Processing Time');
    // }
}

module.exports = { run };