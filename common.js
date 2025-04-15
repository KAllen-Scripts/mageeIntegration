// modules
const axios = require('axios');
const readline = require("readline");
const fs = require('fs');
const FormData = require('form-data');
const sharp = require('sharp');
const streamifier = require('streamifier');
require('dotenv').config()

const accountID = process.env.APP_ACCOUNTID
const username = process.env.APP_USER
const password = process.env.APP_PASSWORD
global.enviroment = process.env.APP_ENVIROMENT;
var accessToken = {}
var authMethod = 1
var adminToken = {}


let remainingTokens = null
let lastTokenRefresh = null

// var logWrite = fs.createWriteStream('./log.txt', {flags: 'a'});

let dateOptions = {
    weekday: "long", year: "numeric", month: "short",  
    day: "numeric", hour: "2-digit", minute: "2-digit"  
}; 

// generic wait for timeout function to handle a delay
const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// gets the access token using stored global variables
const getAdminToken = async () => {
    adminToken = await axios({
        method: 'post',
        headers: {
            'Content-Type': 'application/json'
        },
        url: `https://${global.enviroment}/v1/signin`,
        data: {
            "accountkey": "$admin",
            "email": username,
            "password": password
        }
    }).then(r => {
        return r.data.data.authenticationResult
    })

    await getClientToken()
}

async function getClientToken(){
    accessToken = await axios({
        method: 'post',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${adminToken.accessToken}`
        },
        url: `https://${global.enviroment}/v1/admin/sessions`,
        data: {
            accountkeyAlias: accountID
        }
    }).then(r => {
        return r.data.data
    })
}


async function postImage(imgURL) {
    try {
        const response = await axios.get(imgURL, { responseType: 'arraybuffer' });
        let imageBuffer = Buffer.from(response.data, 'binary');

        // Check if image size exceeds 2MB
        if (imageBuffer.length > 2 * 1024 * 1024) {
            console.log('Image is too large, compressing...');

            imageBuffer = await sharp(imageBuffer)
                .jpeg({ quality: 80 }) // Adjust quality to compress
                .toBuffer();

            // If still too large, reduce quality further
            let quality = 80;
            while (imageBuffer.length > 2 * 1024 * 1024 && quality > 10) {
                quality -= 10;
                imageBuffer = await sharp(imageBuffer)
                    .jpeg({ quality }) 
                    .toBuffer();
            }

            if (imageBuffer.length > 2 * 1024 * 1024) {
                throw new Error('Unable to compress image below 2MB');
            }
        }

        let data = new FormData();
        const imageStream = streamifier.createReadStream(imageBuffer);
        data.append('image', imageStream, { filename: 'image.jpg' });

        const headers = { ...data.getHeaders() };

        return requester('post', `https://${accountID}.webapp-${global.enviroment}/uploads`, data, 5, headers);
    } catch (error) {
        console.error('Error posting image:', error.message);
        throw error;
    }
}


// All purpose requester function. Pass in a method, url, and data object
const requester = async (method, url, data, attempt = 3, additionalHeaders, reAttempt = false) => {

    if(!accessToken.accessToken){await authenticate()}

    if (Math.abs(new Date(accessToken.expiry).getTime() - new Date().getTime()) <= 2 * 60 * 1000) {
        await getAdminToken()
      }

    let headers = additionalHeaders || {'Content-Type': 'application/json'}
    headers.Authorization = 'Bearer ' + accessToken.accessToken

    let sendRequest = {
        method: method,
        headers: headers,
        url: url,
        data: data
    }

    if(remainingTokens <= 3){
        while ((Date.now() - lastTokenRefresh) <= 60000){
            await sleep(100)
        }
        lastTokenRefresh = Date.now()
    }

    console.log(sendRequest)

    let returnVal = await axios(sendRequest).catch(async e=>{
        remainingTokens = e.response.headers['x-ratelimit-remaining']
        if(e?.response?.data?.message == 'jwt expired' || e?.response?.data?.message == 'Invalid admin session'){
            if(authMethod){
                await getAdminToken()
            }else{
                accessToken.accessToken = await askQuestion('Access token expired. Please enter a new one: ')
            }
            return requester(method, url, data)
        } else if (attempt && reAttempt) {
            let tryAgain
            if(attempt >= 2){
                tryAgain = attempt - 1
            } else if (attempt == 1){
                // tryAgain = await askQuestion(`Request Failed. Enter the number of times you want to retry, or 0 to give up: `).then(r=>{return parseInt(r)})
            } else {
                throw e
            }
            await sleep(6000)
            return requester(method, url, data, tryAgain)
        } else {
            // console.log(e)
            throw e
        }
    })


    remainingTokens = returnVal.headers['x-ratelimit-remaining']

    // console.log(returnVal.data)
    return returnVal
    
}


// All purpose loop function. Allows you to loop through any list in Stok.ly
// You pass in a message that is displayed after each item in the list is handled
// A callback allows you to decide what you are doing with each list item
// Progress counter included. Does not show if message is blank
async function loopThrough(message, url, params = '', filter = '', callBack, incrementPage = true) {
    let page = 0
    let done = 0;
    let total
    do {
        let res = await requester('get', `${url}?page=${page}&${params}&filter=${filter}`).then(r => {
            total = r.data.metadata.count
            return r.data
        })
        var length = res.data.length
        if(incrementPage){page += 1}
        for (const item of res.data) {
            // If we want to leave the function early, we can return false from the callback
            var continueLoop = await callBack(item)
            done += 1
            if (message != '') {
                console.log(`${message} ${done}/${total}`)
            }
            if(continueLoop === false){return}
        }
    } while (length > 0)
}

const makeBold = (str)=>{
    return `\x1b[1m${str}\x1b[0m`
}

// Prompts input from user and returns response in form of promise
const askQuestion = (query) => {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });

    const boldQuery = `${makeBold(query)}`; // Add ANSI escape codes for bold text

    return new Promise(resolve => rl.question(boldQuery, ans => {
        rl.close();
        resolve(ans);
    }));
};


const authenticate = async ()=>{
    await getAdminToken()
    // do{
    //     var authenticated = false

    //     try{
    //         authMethod = await askQuestion('How are we authenticating? 0 for token, 1 for admin session: ').then(r=>{return JSON.parse(r)})

    //         if(authMethod){
    //             accountID = await askQuestion('Enter the account ID: ')
    //             username = await askQuestion('Enter your username: ')
    //             password = await askQuestion('Enter your password: ')
    //             await getAdminToken()
    //         } else {
    //             accessToken.accessToken = await askQuestion('Enter the access token: ')
    //         }

    //         let i = await requester('get', `https://${global.enviroment}/v0/items?size=1`, undefined, 0).then(r=>{return r.data.data})
    //         authenticated = true
    //     } catch {
    //         console.log(`${'|'.repeat(80)}\n\nAUTHENTICATION FAILED. TRYING AGAIN\n\n${'|'.repeat(80)}`)
    //     }
    // } while (!authenticated)

    let i = await requester('get', `https://${global.enviroment}/v0/items?size=1`, undefined, 0).then(r=>{return r.data.data})
    authenticated = true

    lastTokenRefresh = Date.now()
    
    console.log(`${'|'.repeat(80)}\n\nAUTHENTICATED\n\n${'|'.repeat(80)}`)
}


module.exports = {
    requester,
    sleep,
    loopThrough,
    getAdminToken,
    askQuestion,
    authenticate,
    postImage,
    makeBold
};