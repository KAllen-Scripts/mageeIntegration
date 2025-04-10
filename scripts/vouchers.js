let common = require('../common.js');
const fs = require('fs');
const csv = require('fast-csv');
process.chdir(__dirname);
let vouchersFileName = '../files/saleorders/250218_FM Gift Vouchers outstanding as at 18.02.25.csv';

async function makeVouchers(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(vouchersFileName)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data', async row => {
            stream.pause()

            try{
                await common.requester('post', 'https://api.stok.ly/v0/credit-notes', {
                    "barcode": row['reference'],
                    "value": {
                        "currency": 'EUR',
                        "amount": row['balance']
                    },
                    "referenceType": 0
                })
            } catch {

            }

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function run(){
    await makeVouchers()
}

module.exports = {
    run
}