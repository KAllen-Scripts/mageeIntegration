let common = require('../common.js');
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);
let bins = '../files/stockStuff/barcode locations.csv';

function generateRange(from, to) {
    // Handle invalid inputs
    if (!from || !to || typeof from !== 'string' || typeof to !== 'string') {
      throw new Error('Both from and to must be non-empty strings');
    }
    
    // If from and to are the same, return just that value
    if (from === to) {
      return [from];
    }
    
    // Extract any common prefix between the 'from' and 'to' values
    let commonPrefixLength = 0;
    while (commonPrefixLength < from.length && 
           commonPrefixLength < to.length && 
           from[commonPrefixLength] === to[commonPrefixLength]) {
      commonPrefixLength++;
    }
    
    const prefix = from.substring(0, commonPrefixLength);
    const fromSuffix = from.substring(commonPrefixLength);
    const toSuffix = to.substring(commonPrefixLength);
    
    // Case 1: The part that varies is purely numeric
    if (/^\d+$/.test(fromSuffix) && /^\d+$/.test(toSuffix)) {
      const start = parseInt(fromSuffix, 10);
      const end = parseInt(toSuffix, 10);
      
      // Ensure end is greater than or equal to start
      if (end < start) {
        throw new Error('The to value must be greater than or equal to the from value');
      }
      
      // Generate all numbers in the range and format them
      const result = [];
      for (let i = start; i <= end; i++) {
        const numStr = i.toString().padStart(fromSuffix.length, '0');
        result.push(prefix + numStr);
      }
      return result;
    }
    
    // Case 2: The part that varies is alphanumeric (like A to AQ)
    // This is more complex, requiring character-by-character incrementation
    
    // Helper function to convert a string to its "numeric" value
    // e.g., "A" -> 1, "B" -> 2, ..., "Z" -> 26, "AA" -> 27, etc.
    function stringToValue(str) {
      let value = 0;
      for (let i = 0; i < str.length; i++) {
        const char = str[i];
        if (/[A-Z]/.test(char)) {
          value = value * 26 + (char.charCodeAt(0) - 'A'.charCodeAt(0) + 1);
        } else if (/[a-z]/.test(char)) {
          value = value * 26 + (char.charCodeAt(0) - 'a'.charCodeAt(0) + 1);
        } else if (/[0-9]/.test(char)) {
          value = value * 10 + parseInt(char, 10);
        } else {
          throw new Error(`Unsupported character: ${char}`);
        }
      }
      return value;
    }
    
    // Helper function to convert a "numeric" value back to a string
    // with the same character types (uppercase/lowercase/digit) as the original
    function valueToString(value, template) {
      let result = '';
      let remaining = value;
      
      for (let i = template.length - 1; i >= 0; i--) {
        const char = template[i];
        if (/[A-Z]/.test(char)) {
          const charValue = (remaining - 1) % 26 + 1;
          remaining = Math.floor((remaining - 1) / 26);
          result = String.fromCharCode('A'.charCodeAt(0) + charValue - 1) + result;
        } else if (/[a-z]/.test(char)) {
          const charValue = (remaining - 1) % 26 + 1;
          remaining = Math.floor((remaining - 1) / 26);
          result = String.fromCharCode('a'.charCodeAt(0) + charValue - 1) + result;
        } else if (/[0-9]/.test(char)) {
          const charValue = remaining % 10;
          remaining = Math.floor(remaining / 10);
          result = charValue.toString() + result;
        }
        
        if (remaining === 0) break;
      }
      
      // If there are still remaining digits, add them to the front
      while (remaining > 0) {
        if (/[A-Z]/.test(template[0])) {
          const charValue = (remaining - 1) % 26 + 1;
          remaining = Math.floor((remaining - 1) / 26);
          result = String.fromCharCode('A'.charCodeAt(0) + charValue - 1) + result;
        } else if (/[a-z]/.test(template[0])) {
          const charValue = (remaining - 1) % 26 + 1;
          remaining = Math.floor((remaining - 1) / 26);
          result = String.fromCharCode('a'.charCodeAt(0) + charValue - 1) + result;
        } else if (/[0-9]/.test(template[0])) {
          const charValue = remaining % 10;
          remaining = Math.floor(remaining / 10);
          result = charValue.toString() + result;
        }
      }
      
      // Ensure the result has the same format as the template
      return result.padStart(template.length, /[A-Z]/.test(template[0]) ? 'A' : 
                             /[a-z]/.test(template[0]) ? 'a' : '0');
    }
    
    // For alphanumeric ranges, we'll try a simplified approach for your case
    // where the prefix is the same and only the last portion changes
    // This works for examples like C07A to C07AQ where only the last part increments
    
    // Special handling for your case (letter sequences)
    if (/^[A-Z]+$/.test(fromSuffix) && /^[A-Z]+$/.test(toSuffix)) {
      const result = [];
      
      // Convert to numeric values
      const startVal = stringToValue(fromSuffix);
      const endVal = stringToValue(toSuffix);
      
      // Generate the range
      for (let i = startVal; i <= endVal; i++) {
        result.push(prefix + valueToString(i, fromSuffix));
      }
      
      return result;
    }
    
    // Handle mixed alphanumeric case by just returning the endpoints
    // This is a fallback for complex cases
    return [from, to];
  }


async function makeBins() {
    let all = {}
    return new Promise((res, rej) => {
        const stream = fs.createReadStream(bins)
            .pipe(csv.parse({
                headers: headers => headers.map(h => h.toLowerCase().trim())
            }))
            // .on('error', error => console.error(error))
            .on('data', async row => {
                stream.pause()

                    if(row['stok.ly location'] != ''){

                        let binsLocations = {
                            "bins": []
                        }
    
                        let binRange = generateRange(row.from, row.to)
    
                        for (const bin of binRange){
                            if(!allBins.includes(`${row['stok.ly location']} ${bin}`.toLowerCase().trim())){
                                binsLocations.bins.push({
                                    "status": "active",
                                    "name": `${row['stok.ly location']} ${bin}`,
                                    "type": row.pickable == 'no' ? 'non_pickable' : 'pickable',
                                    "barcode": bin
                                })
                            }
                        }
                        if(binsLocations.bins.length > 0){await common.requester('patch', 'https://api.stok.ly/v0/locations/b98373ab-9d0b-46d3-974f-baf2d13a1a5d', binsLocations)}
                    }


                stream.resume()
            })
            .on('end', async () => {
                res()
            })
    })
}



let allBins = []
async function run(){
    await common.loopThrough('', `https://api.stok.ly/v0/locations/b98373ab-9d0b-46d3-974f-baf2d13a1a5d/bins`, `size=1000`, '[status]=={active}', (bin)=>{
        allBins.push(bin.name.toLowerCase().trim())
    })

    await makeBins()
}

module.exports = {
    run
}