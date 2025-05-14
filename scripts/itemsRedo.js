const common = require('../common.js');
const fs = require('fs');
const path = require('path');
const fastcsv = require('fast-csv');
const { countries } = require('countries-list');

// File paths
const itemsFilePath = path.resolve(__dirname, `../files - new/data.json`);
const imagesFilePath = path.resolve(__dirname, `../files - new/images.json`);
const protexSKUFolder = path.resolve(__dirname, `../files - new/Product SKU`);
const protexProductFolderPath = path.resolve(__dirname, `../files - new/product`);
const fmFilePath = path.resolve(__dirname, `../files - new/FM/250512_FM Product Data.csv`);
const siteProductsFilePath = path.resolve(__dirname, `../files - new/website/Models.csv`);
const attributeNamesFilePath = path.resolve(__dirname, `../files - new/website/Attribute Names.csv`);
const attributeValuesFilePath = path.resolve(__dirname, `../files - new/website/Attribute Values.csv`);
const stockAttributesFilePath = path.resolve(__dirname, `../files - new/website/Stock Attributes.csv`);
const modelImagesFilePath = path.resolve(__dirname, `../files - new/website/IRP-img-urls-all-time.csv`);
const FMSupplierLookupCSV = path.resolve(__dirname, `../files - new/FM-supplier-code-lookup.csv`);
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






// //magee mappings
// ///////////////////////////////////////////////////////////////////////////////////
// const childMappables = {
//     FM: {
//       'ColourDesc\n(Ref 6)': '3317d32a-3a8f-4a61-a14a-566edf56a470',
//       Brand: '72ab4034-3052-4604-b162-971bbe7910d2',
//       'Season\n(Ref 1)': '60c36253-f5de-415e-8512-9676bac7e4cc',
//       Department: '99d3a71c-aefc-4745-a534-9a4a4b34cb99'
//     },
//     Protex: {
//       'product search 1 description': '99d3a71c-aefc-4745-a534-9a4a4b34cb99',
//       'product colour season/year': '60c36253-f5de-415e-8512-9676bac7e4cc',
//       'Season/Year': '9703945c-5e2e-4256-ab43-5f53cee8864b',
//       'product search 2 description': 'b7e3d108-5631-4d68-b575-6a8344ce4bc3',
//       'product colour search 1 description': '3e0b3ae5-eadd-4b68-9f9d-cda0535a8192'
//     },
//     siteAttributes: { Colour: '3317d32a-3a8f-4a61-a14a-566edf56a470' },
//     siteData: {
//       models_model: 'daba7868-59f7-4718-8030-c919f4a59c34',
//       Models_AdditionalInformation1: '02b2a667-a245-4dcf-bcf9-5b4b26f98176',
//       Models_AdditionalInformation2: '395c42b3-b2f7-4f51-a495-e7ed559b9873'
//     }
//   };

// const parentMappables = {
//     FM: {
//       'retail price (inc vat)': '366aabba-ab23-41a6-abc5-9f8510ccf0a7',
//       'uk wholesale (gbp) (exc vat) (user 3)': '6759c0e7-af2f-4f7a-828c-1f4c8d8fffc9',
//       'eu wholesale (eur) (exc vat) (user 2)': '39098224-1ceb-4e7c-a357-b407eae40a76',
//       'ColourDesc\n(Ref 6)': '3317d32a-3a8f-4a61-a14a-566edf56a470',
//       Colour: '4beea84b-57ea-4e55-8ba1-8746c74e35b1',
//       Brand: '72ab4034-3052-4604-b162-971bbe7910d2',
//       'Season\n(Ref 1)': '60c36253-f5de-415e-8512-9676bac7e4cc',
//       Department: '99d3a71c-aefc-4745-a534-9a4a4b34cb99'
//     },
//     Protex: {
//       'product search 1 description': '99d3a71c-aefc-4745-a534-9a4a4b34cb99',
//       'product colour season/year': '60c36253-f5de-415e-8512-9676bac7e4cc',
//       'Season/Year': '9703945c-5e2e-4256-ab43-5f53cee8864b',
//       'product search 2 description': 'b7e3d108-5631-4d68-b575-6a8344ce4bc3',
//       'product colour search 1 description': '3e0b3ae5-eadd-4b68-9f9d-cda0535a8192',
//       'Product Colour Code': '4beea84b-57ea-4e55-8ba1-8746c74e35b1'
//     },
//     siteAttributes: {
//       pattern: '68b6b4dc-8516-45c3-befe-840038c7c6ca',
//       material: 'e9501203-385e-4a63-b8d3-792760560945',
//       style: '83e1b674-4607-4af9-b5ac-cb81998261dc',
//       Colour: '3317d32a-3a8f-4a61-a14a-566edf56a470'
//     },
//     siteData: {
//       models_model: 'daba7868-59f7-4718-8030-c919f4a59c34',
//       Models_AdditionalInformation1: '02b2a667-a245-4dcf-bcf9-5b4b26f98176',
//       Models_AdditionalInformation2: '395c42b3-b2f7-4f51-a495-e7ed559b9873'
//     }
//   };

// const parentProductAttribute = '52d10681-74a5-4269-9346-57cdc345dea5';
// const sizeAttribute = '6fa5e52c-d856-44a8-ae07-8ce40f5c4618';
// const fitAttribute = '208caa95-7ead-49ab-9818-6af5280bfdf1';
// ///////////////////////////////////////////////////////////////////////////////////




//mageeStaging2 mappings
///////////////////////////////////////////////////////////////////////////////////
const childMappables = {
    FM: {
      'ColourDesc\n(Ref 6)': 'e22a53a8-e335-44f7-9d16-754fbb6e1759',
      Brand: '414a34df-4082-4d2a-bdbe-992601af0ee2',
      'Season\n(Ref 1)': 'bb187fc9-3cd9-4a5a-b526-c4a982063075',
      Department: '2e21f1e5-7292-474e-a0da-1f71224e4f39',
      'Colour Code': '1ac39d5c-4214-4260-a5d6-6c9c3859c668'
    },
    Protex: {
      'product search 1 description': '2e21f1e5-7292-474e-a0da-1f71224e4f39',
      'product colour season/year': 'bb187fc9-3cd9-4a5a-b526-c4a982063075',
      'Season/Year': 'fcd906e7-7b0c-49a5-9590-ea5dfc796af8',
      'product search 2 description': '390b4cc7-3ccc-4221-9c65-687025048aca',
      'product colour search 1 description': '04d359f2-44b5-41d0-96af-1170e3d2363d'
    },
    siteAttributes: { Colour: 'e22a53a8-e335-44f7-9d16-754fbb6e1759' },
    siteData: {
      models_model: 'cecdb526-f465-4e15-97fc-0d13c92008be',
      Models_AdditionalInformation1: '33a4bc17-c027-4870-ad14-dcc41fea13bc',
      Models_AdditionalInformation2: '69b5940f-14d5-424f-91b2-29d7d1a56394'
    }
};

const parentMappables = {
    FM: {
      'retail price (inc vat)': '61e64383-e8f1-416e-8e53-c2243d572c98',
      'uk wholesale (gbp) (exc vat) (user 3)': '85a573c0-268c-4c07-99b3-cf1893ea2783',
      'eu wholesale (eur) (exc vat) (user 2)': 'fdf811d1-72a3-4737-8eb8-31c99d5ce975',
      'ColourDesc\n(Ref 6)': 'e22a53a8-e335-44f7-9d16-754fbb6e1759',
      Colour: '1ac39d5c-4214-4260-a5d6-6c9c3859c668',
      Brand: '414a34df-4082-4d2a-bdbe-992601af0ee2',
      'Season\n(Ref 1)': 'bb187fc9-3cd9-4a5a-b526-c4a982063075',
      Department: '2e21f1e5-7292-474e-a0da-1f71224e4f39'
    },
    Protex: {
      'product search 1 description': '2e21f1e5-7292-474e-a0da-1f71224e4f39',
      'product colour season/year': 'bb187fc9-3cd9-4a5a-b526-c4a982063075',
      'Season/Year': 'fcd906e7-7b0c-49a5-9590-ea5dfc796af8',
      'product search 2 description': '390b4cc7-3ccc-4221-9c65-687025048aca',
      'product colour search 1 description': '04d359f2-44b5-41d0-96af-1170e3d2363d',
      'Product Colour Code': '1ac39d5c-4214-4260-a5d6-6c9c3859c668'
    },
    siteAttributes: {
      pattern: 'b02a1abb-90e1-4617-af56-3ef9a44bc1a8',
      material: '81d4e519-c623-4711-9fc2-5e5bd94c508d',
      style: '2e23f8a2-4d70-4728-9fa0-a930d73beb6b',
      Colour: 'e22a53a8-e335-44f7-9d16-754fbb6e1759'
    },
    siteData: {
      models_model: 'cecdb526-f465-4e15-97fc-0d13c92008be',
      Models_AdditionalInformation1: '33a4bc17-c027-4870-ad14-dcc41fea13bc',
      Models_AdditionalInformation2: '69b5940f-14d5-424f-91b2-29d7d1a56394'
    }
};

const taxClassMapping = {
    "23": "c25a24dd-5ff3-43bf-be8c-348712dfc64d",
    "13.5": "7cc51633-69f3-4eaf-8604-b99eb0e91daf",
    "9": "0b1bb996-5745-447d-bd87-2339cbcd7dfa",
    "0": "29b1e356-0e1c-46e1-bff6-a183fab65b22"
}

const parentProductAttribute = '18db2031-d491-440c-a39b-9977e5411152';
const sizeAttribute = 'd71029a7-3e16-4564-95b4-4b8b329cd473';
const fitAttribute = 'f618d86c-24cb-4a14-a7f0-e5c98b4dfa3a';
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
let barcodeTracker = [];

const createOnly = false;


//Will loop through these to remove fit letter from size, since the data is inconsistent
const fitLookup = {
    R: "Regular",
    S: 'Short',
    L: 'Long',
    XS: 'Extra Short',
    XL: 'Extra Long'
};

function getCountryCode(countryName) {
    if(countryName == undefined){return}
        
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
            itemList[item.sku.toLowerCase().trim()] = item.itemId
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
                    if(!barcodeTracker.includes(ean)){
                        if (ean) productData[productCode].variants[formattedVariantKey].barcodes.push(ean)
                        barcodeTracker.push(ean)
                    }
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
                
                if(!barcodeTracker.includes(systemBarcode)){
                    if (systemBarcode && !productData[productCode].variants[formattedVariantKey].barcodes.includes(systemBarcode)) {
                        productData[productCode].variants[formattedVariantKey].barcodes.push(systemBarcode);
                    }
                    barcodeTracker.push(systemBarcode)
                }

                if(!barcodeTracker.includes(gtin1)){
                    if (gtin1 && !isNaN(gtin1) && !productData[productCode].variants[formattedVariantKey].barcodes.includes(gtin1)) {
                        productData[productCode].variants[formattedVariantKey].barcodes.push(gtin1);
                    }
                }

                if(!barcodeTracker.includes(gtin2)){
                    if (gtin2 && !isNaN(gtin2) && !productData[productCode].variants[formattedVariantKey].barcodes.includes(gtin2)) {
                        productData[productCode].variants[formattedVariantKey].barcodes.push(gtin2);
                    }
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


    for (const parent of Object.keys(productData)) {
        const type = productData[parent].attributes?.FM?.['Type'];

        if (type && itemTypes?.[type?.toLowerCase()?.trim()] == undefined){
            try{
                await common.requester('post', `https://${global.enviroment}/v0/item-types`, {"name":type}).then(r=>{
                    itemTypes[type.toLowerCase().trim()] = r.data.data.id
                })
            } catch {console.log(type)}
        }
        for (const child of Object.keys(productData[parent].variants)){
            const type = productData[parent].variants[child].attributes?.FM?.['Type'];
            if (type && itemTypes?.[type?.toLowerCase()?.trim()] == undefined){
                try{
                    await common.requester('post', `https://${global.enviroment}/v0/item-types`, {"name":type}).then(r=>{
                        itemTypes[type.toLowerCase.trim()] = r.data.data.id
                    })
                } catch {
                    console.log(child)
                    console.log(productData[parent].variants[child].attributes)
                }
            }
        }
    }

    console.log('Creating items from product data...');
    const createdItems = [];

    let promiseArr = []
    for (const parent of Object.keys(productData)) {
        promiseArr.push(processItem(parent))
        if (promiseArr.length >= (createOnly ? 1 : 3)){
            await Promise.all(promiseArr)
            promiseArr = []
        }
    }

    await Promise.all(promiseArr)

    fs.writeFileSync('./failed.txt', failedList.join('\n'), 'utf8');



    async function processItem(parent){
        try{
            console.log(`Processing parent product: ${parent}`);
            const parentProduct = productData[parent];

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
                typeId: itemTypes?.[parentProduct?.attributes?.FM?.['Type']?.toLowerCase()?.trim()],
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

                // Create the child item data
                let childData = {
                    "format": 0,
                    "name": Object.keys(parentProduct.variants).length > 1 ? variant.name : parentProduct.name,
                    "sku": Object.keys(parentProduct.variants).length > 1 ? child : parentProduct.singleItemSKU,
                    "barcodes": variant.barcodes || [],
                    "attributes": attributes,
                    typeId: itemTypes?.[variant?.attributes?.FM?.['Type']?.toLowerCase()?.trim()],
                    taxClassId: taxClassMapping[variant?.attributes?.FM?.['Vat Rate']?.toString().toLowerCase().trim()],
                    appendAttributes: true,
                    harmonyCode: variant?.attributes?.FM?.['Intrastat'],
                    countryOfOrigin: getCountryCode(variant?.attributes?.FM?.["COO\n(Ref 7)"])
                };
                try{childData.salePrice = {amount: parseFloat(((variant.attributes.FM['Retail Price\n(Inc Vat)'] / (1 + (parseFloat(variant.attributes.FM['Vat Rate']) / 100)))).toFixed(5)), currency: "GBP"}}catch{}
                if (isNaN(childData.salePrice)){delete childData.salePrice}

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
                let childId
                if (createOnly && itemList[childData.sku.toLowerCase().trim()]){
                    childId = itemList[childData.sku.toLowerCase().trim()]
                } else {
                    try{
                        childId = await processChildItem(childData, variant, parent);
                    } catch (e) {
                        childId = await common.requester('get', `https://api.stok.ly/v0/items?filter=[sku]=={@string;${childData.sku}}%26%26[status]!={1}`).then(r=>{
                            return r.data.data[0].itemId
                        })
                        if(childId == undefined){throw e}
                    }
                    
                }
                parentData.variantItems.push(childId);
                console.log(`Created child item: ${child}`);
            }
            
            if (singleProduct) {
                return;
            }
            
            if (createOnly && !itemList[parentData.sku.toLowerCase().trim()]){
                await processParentItem(parentData, parentProduct);
            } else if (!createOnly) {
                await processParentItem(parentData, parentProduct);
            }

            } catch (e) {
                failedList.push(parent);
                
                console.error('Error processing item:', parent);
                
                // Check if it's an Axios error with response data
                if (e.isAxiosError && e.response) {
                    console.error('Status:', e.response.status);
                    console.error('Status Text:', e.response.statusText);
                    console.error('Response Data:', JSON.stringify(e.response.data, null, 2));
                    console.error('Request URL:', e.config.url);
                    console.error('Request Method:', e.config.method.toUpperCase());
                    console.error('Request Data:', e.config.data, null, 2);
                } else {
                    // For non-Axios errors, log the full error
                    console.error('Error:', e);
                    console.error('Stack:', e.stack);
                }
                
                // await common.askQuestion(e);
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
        // await processImagesCSV();
        console.timeEnd('Process Images CSV');
        
        console.time('Save Product Data');
        await saveDataToFile();
        console.timeEnd('Save Product Data');

        // console.time('Make Items');
        await makeItems();
        // console.timeEnd('Make Items');
        
        console.log('Product Data processing completed successfully');
    // } catch (error) {
    //     console.error('Error during data processing:', error);
    // } finally {
    //     console.timeEnd('Total Processing Time');
    // }

}

module.exports = { run };
