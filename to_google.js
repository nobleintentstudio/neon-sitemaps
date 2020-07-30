const { GoogleSpreadsheet } = require('google-spreadsheet');
const google_creds = require('../google.json');

const fs = require('fs');
const utils = require('../utils.js');

//step 1
// pick a url from sitemap_all.csv
const find_next_unsynced_url = async function() {
  await utils.later(100);
  var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all.csv');
  var found_link = false;

  for(let possible_link of sitemap_urls) {
    if(!found_link && possible_link.sheet_status == '' ) {
      found_link = possible_link.url;
    }
  }
  return found_link;
}

//step 2
// check the https status
// record a non 200
const check_url_status = async function(url) {

  var meta = await utils.getMetaInfo(url);
  var status = parseInt(meta.status);
  return status;

}
//step 2
//find the associated json
const find_url_json = async function(url) {
  let dir = await utils.determineDirectoryFromUrl(url);
  let json = false;
  try {
    json = await utils.readJson(dir+'stats.json');
  } catch(err) {
    console.log('*************ERROR*************');
    console.log('no stats file found');
    console.log('*******************************');
  }
  
  return json;
}

//step 3
//send the json to google sheet
//1ZiwEAAqVtHiuBUvEZ9WerK2WATXt4iVcMyS1AigGVKw
/*
url host  path  status  plan  screenshot  visits  canonical title timestamp lh_mobile_score lh_desktop_score  lh_page_speed_desktop lh_a11y_desktop lh_seo_desktop  lh_best_practices_mobile  lh_page_speed_desktop lh_a11y_desktop lh_seo_desktop  lh_best_practices_desktop 
*/
const non_200_data_to_google = async function(json){

}
const json_to_google = async function(json) {

  let base ='https://nobleintentstudio.com/neon-sitemaps/' + json.full_host + json.path;
  let status = json.http_status;

  sheet_status = 'complete';
  if(status == '' || !status){
    console.log('*************ERROR*************');
    console.log('no http_status found');
    console.log('*******************************');
    status = json.status;
    sheet_status = 'incomplete';
  }
  

  if(!json.lighthouse.desktop.seo) {
    sheet_status = 'incomplete';
  }

  if(sheet_status!='complete') {
    console.log('*************error*************');
    console.log('incomplete data for',json.url);
    console.log('*******************************');
  }

  var data_to_write = {
    url: json.url,
    host: json.full_host,
    path: json.path,
    status: status,
    screenshot_mobile: '=HYPERLINK("' + base + 'lighhouse_mobile.jpg", "mobile screenshot")',
    screenshot_desktop: '=HYPERLINK("' + base + 'lighhouse_desktop.jpg", "desktop screenshot")',
    //visits: '',
    canonical: json.canonical_url,
    title: json.title,
    timestamp: new Date(json.time).toString(),
    lh_mobile_score: Math.round(100*(json.lighthouse.mobile.page_speed+json.lighthouse.mobile.a11y+json.lighthouse.mobile.seo+json.lighthouse.mobile.best_practices)/4)/100,
    lh_desktop_score: Math.round(100*(json.lighthouse.desktop.page_speed+json.lighthouse.desktop.a11y+json.lighthouse.desktop.seo+json.lighthouse.desktop.best_practices)/4)/100,
    lh_page_speed_desktop: json.lighthouse.desktop.page_speed,
    lh_a11y_desktop: json.lighthouse.desktop.a11y,
    lh_seo_desktop: json.lighthouse.desktop.seo,
    lh_best_practices_desktop: json.lighthouse.desktop.best_practices,
    lh_page_speed_mobile: json.lighthouse.mobile.page_speed,
    lh_a11y_mobile: json.lighthouse.mobile.a11y,
    lh_seo_mobile: json.lighthouse.mobile.seo,
    lh_best_practices_mobile: json.lighthouse.mobile.best_practices,
    sheet_status: sheet_status
  };

  // console.log('would write:');
  // console.log(data_to_write);


  let doc = new GoogleSpreadsheet('1ZiwEAAqVtHiuBUvEZ9WerK2WATXt4iVcMyS1AigGVKw');
  await doc.useServiceAccountAuth(google_creds); 
  await doc.loadInfo(); // loads document properties and worksheets
  const data_sheet = doc.sheetsByIndex[0];

  var add = await data_sheet.addRow(data_to_write);

  

  return data_to_write;

}

//step 4 record that the url has been processed in the csv

const record_completion = async function(url, status) {
  await utils.later(100);
  var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all.csv');
  var found_link = false;

  for(let possible_link of sitemap_urls) {
    if(!found_link && possible_link.url == url) {
      found_link = possible_link.url;
      possible_link.sheet_status = status;
    }
  }
  if(found_link) {
    var csv_data = utils.jsonToCsv(sitemap_urls);
    utils.dataToFile(csv_data,'./sitemap_all.csv');
  } else {
    console.log('*************ERROR*************');
    console.log('no matching link found');
    console.log('*******************************');
  }
  return found_link;

}

//step 5 loop it all


const add_url_stats_to_google = async function(url) {
  let status = await check_url_status(url);
  let full_host = await utils.determineHost(url);
  let path = await utils.determinePath(url);
  let json = {url:url, full_host:full_host, path:path, status:status};
  let url_stats;
  if(status == 200) {
    url_stats = await find_url_json(url);
    json = Object.assign(json, url_stats);
  } else {
    json.status = status;
    console.log('*************warning*************');
    console.log('non 200 status code for',url);
    console.log('*******************************');
  }



  if(url_stats) {
    let result = await json_to_google(json);
    await record_completion(url, result.sheet_status);
    return result.sheet_status;
  } else {
    json.sheet_status = 'non_200'
    await json_to_google(json);
    await record_completion(url, 'errored');
    return 'errored';
  }
}

const record_sitemap_data = async function()  {

  let starttime = new Date().getTime();
  async function checkForAnotherURLToScrape() {
     let starttime2 = new Date().getTime();
    await utils.wait(2000);
    let next_url = await find_next_unsynced_url();
    console.log('next url  |', next_url);
    if(next_url) {
      let sheet_status = await add_url_stats_to_google(next_url);
      if(sheet_status == 'incomplete') {

      }
      let elapsed = utils.computeSecondsDiff(starttime2,new Date().getTime());
      console.log('recorded  | '+next_url+' | ' + elapsed+' s');
      await checkForAnotherURLToScrape();
    } else {
      let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
      console.log('all urls recorded | ' + elapsed+' s');
    }
  }

  await checkForAnotherURLToScrape();
}


const start = async function() {
  // let task = args.task || args.t || 'scrape';
  // if(task == 'scrape') {
  //   await record_sitemap_data();
  // } else if(task == 'mark') {
  //   await mark_all_completions();
  // } else if(task == 'sitemap') {
  //   await combine_neon_sitemaps();
  // } else if(task == 'new_sitemaps') {
  //   await get_neon_sitemaps();
  // } else if(task == 'dedupe') {
  //   await remove_duplicates_from_sitemap();
  // } else if(task=='tojson') {
  //   await turn_sitemap_into_json();
  // }

  record_sitemap_data();

  // get_neon_sitemaps();
  // crawl_neon_sites();
  // 
  
  console.log('--------waited----------------');
}

start();
