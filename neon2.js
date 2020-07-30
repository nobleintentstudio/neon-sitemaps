const fs = require('fs');

var _ = require('lodash');
const shuffle = require('shuffle-array');

const { GoogleSpreadsheet } = require('google-spreadsheet');
const google_creds = require('../google.json');

let doc;
let data_sheet;

const setup_google_sheet = async function() {
  doc = new GoogleSpreadsheet('1ZiwEAAqVtHiuBUvEZ9WerK2WATXt4iVcMyS1AigGVKw');
  await doc.useServiceAccountAuth(google_creds); 
  await doc.loadInfo(); // loads document properties and worksheets
  data_sheet = doc.sheetsByIndex[0];
}

const utils = require('../utils.js');
const scrape = require('../scrape.js');

const args = require('minimist')(process.argv.slice(2));
const URL = require('url');
let rootDomains = [
  'neonone.com',
  'www.arts-people.com',
  'www.neoncrm.com',
  'rallybound.com',
  'www.civicore.com',
  'neoncrm.com'
];
let rootUrls = [
  'https://neonone.com',
  'https://www.arts-people.com',
  'https://www.neoncrm.com',
  'https://rallybound.com',
  'https://www.civicore.com',
];


var total_sitemap_link_count = 0;

const has_no_status = function(row) {
  return !row.crawl_status;
}
const needs_rescrape = function(row) {

  return row.crawl_status == 'incomplete' || row.crawl_status == 'missing' || !row.crawl_status || has_no_sheet_status(row);

  
}
const has_no_sheet_status = function(row) {
  return !row.sheet_status; // && (row.crawl_status == 'complete' || row.crawl_status == 'non_200');
}
const has_3xx = function(row) {
  return row.status.indexOf('3') == 0;
}

const translate_json_to_google_sheet_format = function(json) {
  let base ='https://nobleintentstudio.com/neon-sitemaps/' + json.full_host + json.path;

  let timestamp = json.timestamp;
  if(!timestamp && json.time) {
    timestamp = new Date(json.time).toString();
  } else if(!timestamp) {
    timestamp = new Date().toString();
  }
  return {
    url: json.url,
    host: json.full_host,
    path: json.path,
    status: json.status,
    screenshot_mobile: '=HYPERLINK("' + base + 'lighhouse_mobile.jpg", "mobile screenshot")',
    screenshot_desktop: '=HYPERLINK("' + base + 'lighhouse_desktop.jpg", "desktop screenshot")',
    //visits: '',
    timestamp: timestamp,
    canonical: json.canonical_url,
    title: json.title,
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
    sheet_status: json.sheet_status
  };
}
const upload_url_and_status_to_google_sheet = async function(url) {
  await check_url_for_status_and_completeness(url);

  let starttime = new Date().getTime();
  // get necessary data

  let existing_row = await utils.get_csv_row_by_url(url, 'sitemap_all.csv');
  let existing_status = existing_row.status;
  // console.log('existing_status?', existing_status.status);

  let full_host = await utils.determineHost(url);
  let path = await utils.determinePath(url);
  let baseJson = {url:url, full_host:full_host, path:path, status:existing_status, timestamp: new Date().toString()};

  if(existing_status) {
    if(existing_status == 200 || existing_status == '200') {
      let existingStats = await utils.findExistingURLStatsJSON(url);
      if(existingStats) {
        baseJson.sheet_status = 'complete';
        if(existingStats.time){
          baseJson.timestamp = new Date(existingStats.time).toString();
        }
        
        let statsObject = Object.assign(baseJson,existingStats);
        let google_format = translate_json_to_google_sheet_format(statsObject);

        var add = await data_sheet.addRow(google_format);
        
      } else {
        // skip??
        baseJson.sheet_status = 'skipped_no_stats';
      }
    } else {
      //300??
      baseJson.sheet_status = 'complete';
      let statsObject = Object.assign(baseJson);
      // let google_format = translate_json_to_google_sheet_format(statsObject);
      let add = await data_sheet.addRow(statsObject);
    }

  } else {
    //skip??
    baseJson.sheet_status = 'skipped_no_status';
  }

  let row_data = {url:url, sheet_status: baseJson.sheet_status};
  await utils.update_csv_row_by_url(url, row_data, 'sitemap_all.csv');

  let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
  console.log('uploaded_url_stats_to_google | ' + elapsed+' s');

  return true;
}

const scrape_neon_url = async function(url) {

}

const rescrape_incomplete_url = async function(url) {

  var starttime = new Date().getTime();

  let existing_row = await utils.get_csv_row_by_url(url, 'sitemap_all.csv');

  let status_object =  await utils.getHTTPStatus(url);
  let status = status_object.status;

  if(status == 200 || status == '200') {

    let page_info = await scrape.getInfo({url:url,neon:true,local:true});

    if(existing_row.crawl_status != 'complete') {
      let all_links_on_page = page_info.links;

      for(let link_from_page of all_links_on_page) {
        let csv_file = utils.is_external_link(link_from_page, rootDomains) ? 'sitemap_all_external.csv' : 'sitemap_all.csv';
        await utils.update_csv_row_by_url(link_from_page, {url:link_from_page}, csv_file);
      }
    }

    //if it was a 3xx or 200, we should do the other things too
    await check_url_for_status_and_completeness(url);
    await upload_url_and_status_to_google_sheet(url);
    return utils.wait(10);
  } else if(status.indexOf('3') == 0) {
    //if it was a 3xx we should do the other things too
    await check_url_for_status_and_completeness(url);
    await upload_url_and_status_to_google_sheet(url);
    return utils.wait(10);
  } else {
    //leave it alone
    return utils.wait(10);
  }

  
}

const check_url_for_status_and_completeness = async function(url) {
  let starttime = new Date().getTime();
  // check status
  let status_object =  await utils.getHTTPStatus(url);
  let status = status_object.status;
  if(status_object.location) {
    let csv_file = utils.is_external_link(status_object.location, rootDomains) ? 'sitemap_all_external.csv' : 'sitemap_all.csv';
    await utils.update_csv_row_by_url(status_object.location, {url:status_object.location}, csv_file);
  }
  // check crawl completeness
  // complete, incomplete, missing, not_200
  let crawl_status;

  if(status != 200){
    crawl_status = 'not_200';
  } else {
    crawl_status = await scrape.get_existing_crawl_status(url);
  }

  // record crawl_status
  // record status
  let row_data = {url:url, status: status, crawl_status: crawl_status};
  await utils.update_csv_row_by_url(url, row_data, 'sitemap_all.csv');

  let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
  console.log('checked status and completeness | ' + elapsed+' s');

  return true;

}

const find_an_unchecked_url_in_sitemap = async function(elibilityFunction) {
  // await utils.wait(100);

  var rows = await utils.readCsvFileAsJson('sitemap_all.csv');
  var found_matching_url = false;

  var stats = {total:rows.length,ineligible:0};

  for(let row of rows) {
    let eligibile = elibilityFunction(row);
    if(!found_matching_url && eligibile) {
      found_matching_url = row.url;
    }
    if(!eligibile) {
      stats.ineligible++;
    }
  }
  console.log(stats.ineligible, 'of',stats.total,'links finished', new Date().toString());
  return found_matching_url;
}

const do_something_to_all_sitemap_links = async function(toDoFunction, elibilityFunction, delay) {
  if(!delay) {
    delay = 0;
  }
  let starttime = new Date().getTime();
  async function findAnother() {
    let starttime2 = new Date().getTime();
    
    let next_url = await find_an_unchecked_url_in_sitemap(elibilityFunction);
    console.log('next url  |', next_url);
    if(next_url) {
      await toDoFunction(next_url);
      if(delay) {
        await utils.wait(delay);
      }
      let elapsed = utils.computeSecondsDiff(starttime2, new Date().getTime());
      console.log(next_url +' completed | ' + elapsed+' s');
      await findAnother();
    } else {
      let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
      console.log('all urls complete | ' + elapsed+' s');
    }
  }
  await findAnother();
}


const start = async function() {
  let task = args.task || args.t || 'scrape';
  if(task == 'completeness') {
    await do_something_to_all_sitemap_links(check_url_for_status_and_completeness, has_no_status, 0);
  } else if(task == 'google') {
    await setup_google_sheet();
    await do_something_to_all_sitemap_links(upload_url_and_status_to_google_sheet, has_no_sheet_status, 1000);
  } else if(task == 'rescrape') {
    await setup_google_sheet();
    await do_something_to_all_sitemap_links(rescrape_incomplete_url, needs_rescrape, 500);
  }

  // get_neon_sitemaps();
  // crawl_neon_sites();
  // 
  
  console.log('--------waited----------------');
}

start();