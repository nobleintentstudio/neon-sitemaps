//takes a sitemap url
//and finds sub sitemaps and compiles all links found inside of <loc> xml

//$: node _sitemap.js -u https://www.neoncrm.com/sitemap_index.xml

const myArgs = process.argv.slice(2);
const urlToCheck = myArgs[0];
const fs = require('fs');

var _ = require('lodash');
const shuffle = require('shuffle-array');

const { GoogleSpreadsheet } = require('google-spreadsheet');
const google_creds = require('../google.json');


const listSitemapLinks = require('../utils.js').listSitemapLinks;
const jsonToCsv = require('../utils.js').jsonToCsv;
const determineDirectoryFromUrl = require('../utils.js').determineDirectoryFromUrl;
const determineHost = require('../utils.js').determineHost;
const dataToFile = require('../utils.js').dataToFile;

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



//step 1, get sitemaps for each one, for example: 
//$: node neon.js --u https://neonone.com/sitemap_index.xml
const get_neon_sitemaps = async function() {
  let url = args.url || args.u;
  let quiet = args.quiet || args.q;
  
  var pageUrls = [];
  var sitemapUrls = [url];

  function writeUrlsToCsv() {
    var json = [];
    pageUrls.forEach(async function(link){
      json.push({url:link,status:'',crawl_status:''});
    });

    var csvData = jsonToCsv(json);
    var domain = determineHost(url);
    dataToFile(csvData, './' + domain + '/sitemap.csv');
  }

  async function crawlNextSitemap() {
    let nextSiteMap = (sitemapUrls.length) ? sitemapUrls[0] : false;
    if(nextSiteMap) {
      sitemapUrls = sitemapUrls.slice(1,sitemapUrls.length);
      console.log('checking sitemap', nextSiteMap, sitemapUrls.length, 'more to go');
      
      let sitemapLinks = await listSitemapLinks(nextSiteMap, quiet);

      for (const link of sitemapLinks) {
        if(link.indexOf('sitemap.xml') > -1) {
          console.log('new sitemap found', link);
          sitemapUrls.push(link);
        } else {
          pageUrls.push(link);
        }
      }
      
      crawlNextSitemap();

    } else {
      console.log('no more sitemaps to check');
      writeUrlsToCsv();
      return false;
    }
  }
  await crawlNextSitemap();
}

//step 2, crawl each url from the sitemap, record any newly found urls that have the same "domain" (not full hostname), then write that out to "crawled_urls.csv"

const crawl_neon_site = async function(root_url) {
  // return new Promise(function(resolve) {
    console.log('==========================starting to crawl ' + root_url);
    var total_fetches = 0;
    //use url to find the correct folder to find a sitemap.csv
    var path = utils.determineHost(root_url);
    var root_domain = utils.determineHost(root_url);
    //read in the csv as a json array
    let sitemap = await utils.readCsvFileAsJson(path+'/sitemap.csv');
    let sitemap_urls = []; //populate from sitemap.csv, and add all internal links not already present
    for(const page of sitemap) {
      if(sitemap_urls.indexOf(page.url) < 0){
        sitemap_urls.push(page.url);
      }
    }

    let scraped_urls = {internal:[],external:[]}; //for final csv readout

    let scraped_url_list = []; //url,url
    let non_sitemap_urls = []; //{url,status}


    var i = 1;
    const get_url_data = async function(page_url) {
      i++;
      var starttime = new Date().getTime();

      if(scraped_url_list.indexOf(page_url) < 0) {
        var meta = await utils.getMetaInfo(page_url);
        total_fetches++;
        var status = parseInt(meta.status);
        
        // console.log('unscraped sitemap url saved', page_url, status);
        scraped_urls.internal.push({url:page_url,status:meta.status});
        scraped_url_list.push(page_url);
        // if(sitemap_urls.indexOf(page_url)<0){
        //   console.log('new internal url added to sitemap', page_url, status);
        //   sitemap_urls.push(page_url);
        // }

        if(status == 200) {
          // if(i<5) {
          let page_info = await scrape.getInfo({url:page_url,skip_marking:true,prefetched_meta:meta});
          // }
          
          let all_links_on_page = page_info.links;
          for(let link_from_page of all_links_on_page) {
            // console.log('found ',link_from_page, 'on page');
            if(scraped_url_list.indexOf(link_from_page) < 0) {
              // console.log('found unscraped link on page:',link_from_page);
              if(utils.determineHost(link_from_page) == root_domain) {
                //dealing with an internal link
                // console.log('was internal, will add? ',sitemap_urls.indexOf(link_from_page)<0);
                // await get_url_data(link_from_page);
                if(sitemap_urls.indexOf(link_from_page)<0){
                  console.log('internal url added to sitemap:', link_from_page);
                  sitemap_urls.push(link_from_page);
                }
              }  else {

                //dealing with an external link
                //get the status and go
                if(link_from_page.indexOf('#') > 0) { //ingoring query strings
                  link_from_page=link_from_page.split('#')[0];
                }
                if(link_from_page.indexOf('?') > 0) { //ingoring query strings
                  link_from_page=link_from_page.split('?')[0];
                }

                // console.log('an unscraped external url was found, lets save it', link_from_page);
                let link_from_page_meta = await utils.getMetaInfo(link_from_page);
                total_fetches++;
                scraped_urls.external.push({url:link_from_page,status:link_from_page_meta.status});
                scraped_url_list.push(link_from_page);

              }
             
            }
          
          }
        }

      }

      let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
      console.log(page_url + 'crawled and scraped | ' + elapsed+' s | ');
    }


    function find_next_url() {
      
      var found_link = false;
      for(let possible_link of sitemap_urls) {
        if(scraped_url_list.indexOf(possible_link) < 0 && !found_link) {
          found_link = possible_link;
        }
      }
      console.log('looking for the next url in the list, found:',found_link);
      return found_link;
    }

    const recursively_check_urls = async function() {
      
      await utils.wait(100);
      console.log('---');
      console.log(i, 'of', sitemap_urls.length); 

      let next_url = find_next_url();
      if(next_url) {
        await get_url_data(next_url);
        recursively_check_urls();
      } else {
        console.log('no url was found?', scraped_url_list.length, 'v',sitemap_urls.length);
        let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());

        var internal_csv = utils.jsonToCsv(scraped_urls.internal);
        dataToFile(internal_csv, path+ '/internal_links.csv');

        var external_csv = utils.jsonToCsv(scraped_urls.external);
        dataToFile(external_csv, path+ '/external_links.csv');

      }
      return await utils.later(1);
    }
    
    var starttime = new Date().getTime();
    await recursively_check_urls();
    let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());


    
    console.log(root_url + 'crawled and scraped | ' + elapsed+' s | ',total_fetches,' urls fetched for ',sitemap_urls.length,' final sitemap links,',scraped_url_list.length,' total links crawled');
  //   resolve();
  // });   

    return await utils.later(1);
}


//something is wrong here and it is not actually async
const crawl_neon_sites = async function(){
  // let n1 = await crawl_neon_site(rootUrls[0]);
  // let ap = await crawl_neon_site(rootUrls[1]);
  // let neoncrm = await crawl_neon_site(rootUrls[2]);
  let rallybound = await crawl_neon_site(rootUrls[3]); //rallybound
  // let civicore = await crawl_neon_site(rootUrls[4]);
}


const remove_column_values = async function(column_name) {
  console.log('removing', column_name);
  let csv_filename = 'sitemap_all.csv'
  var sitemap_urls = await utils.readCsvFileAsJson(csv_filename);
  console.log(sitemap_urls.length, 'clearing column', column_name);
  sitemap_urls = _.map(sitemap_urls,function(row){
    row[column_name] = '';
    return row;
  });


  var site_csv = await utils.jsonToCsv(sitemap_urls);
  return await utils.dataToFile(site_csv,'./'+csv_filename);
}
//instead lets get all the sub-sitemaps, and get a super sitemap inplace
//we should also keep track of what has been checked out so it can be skipped
//but we can only skip if we also write down the new internal urls as well as log the external ones.
//we should also probably do some kind of shuffling or maybe a few at once.

const combine_neon_sitemaps = async function() {
  var super_site_map = [];
  for(const site of rootUrls) {
    console.log('sitemap for:', site);
    var dir = utils.determineDirectoryFromUrl(site);
    if (fs.existsSync(dir)){
      var lines = await utils.readCsvFileAsJson(dir+'sitemap.csv');
      lines = _.sortBy(lines, 'url');
      console.log(lines.length, 'initial urls');
      lines = _.uniqBy(lines, 'url');
      console.log(lines.length, 'final urls');
      var site_csv = await utils.jsonToCsv(lines);
      await utils.dataToFile(site_csv,'./'+dir+'sitemap.csv');

      for(line of lines) {
        // line.sheet_status = '';
        // line.status = '';
        line.crawl_status = '';
        super_site_map.push(line);
      }
    }
  }
  console.log('super sitemap')
  super_site_map = _.sortBy(super_site_map, 'url');
  console.log(super_site_map.length, 'initial urls');
  super_site_map = _.uniqBy(super_site_map, 'url');
  console.log(super_site_map.length, 'final urls');

  var super_site_map_csv = await utils.jsonToCsv(super_site_map);
  return await utils.dataToFile(super_site_map_csv,'./sitemap_all.csv');
}

const add_url_to_child_sitemap = async function (url) {
  console.log(url, 'to child sitemap', utils.determineHost(url));
  var dir = utils.determineHost(url);
  var sitemap_urls = await utils.readCsvFileAsJson(dir+'/sitemap.csv');
  sitemap_urls.push({url:url});
  var site_csv = await utils.jsonToCsv(sitemap_urls);
  return await utils.dataToFile(site_csv,'./'+dir+'/sitemap.csv');

}

const modify_url_in_child_sitemap = async function(url,status,crawl_status) {
  var dir = utils.determineHost(url);
  var sitemap_urls = await utils.readCsvFileAsJson(dir+'/sitemap.csv');
  if(!crawl_status) {
    crawl_status = 'finished';
  }
  if(url && status){
    var found_link = false;
    for(let possible_match of sitemap_urls) {
      if(!found_link && possible_match.url == url) {
        possible_match.status = status;
        possible_match.crawl_status = crawl_status;
        found_link = true;
      }
    }
    if(found_link) {
      var site_csv = await utils.jsonToCsv(sitemap_urls);
      await utils.dataToFile(site_csv,'./'+dir+'/sitemap.csv');  
    }
    
  }

  return await utils.later(10);

  
}

const get_csv_row_by_url = async function(url, csv_file) {
  var exists = await check_for_url_in_sitemap(url);
  if(exists) {
    
    var sitemap_urls = await utils.readCsvFileAsJson(csv_file);
    var found_link = false;
    // console.log('checking for ', url, in 'csv_file');

    for(let possible_match of sitemap_urls) {
      if(possible_match.url == url) {
        found_link = possible_match;
      }
    }
    // console.log('found?', found_link);
    return found_link;

  }
  return row;
}

const update_csv_row_by_url = async function(url,data,csv_file) {

  if(!csv_file) {
    let external = is_external_link(url);
    csv_file = external ? 'sitemap_all_external.csv' : 'sitemap_all.csv';
  }

  await utils.wait(1000);

  var base_row = {url:url,status:'',crawl_status:'',sheet_status:''};
  var existing_row = await get_csv_row_by_url(url, csv_file);
  var final_row = Object.assign(base_row,existing_row,data);
  // console.log(base_row,existing_row,final_row);
  if(existing_row) {
    var sitemap_urls = await utils.readCsvFileAsJson(csv_file);
    let found_link = false;
    let i = 0;
    for(let possible_match of sitemap_urls) {
      if(!found_link && possible_match.url == url) {
        sitemap_urls[i] = final_row
        found_link = true;
      }
      i++;
    }
    if(found_link) {
      // console.log('found a link');
      var site_csv = await utils.jsonToCsv(sitemap_urls);
      await utils.dataToFile(site_csv,csv_file);  
    }
    return await utils.later(10);
  } else {
    await utils.appendRowToCSV(final_row, csv_file);
  }
}
const modify_url_in_sitemap = async function(url, data) {

  // var data = {status: status, external:external, crawl_status: crawl_status}

  //if external, then we should add to the external list
  //if status, then we need to update the row
  //otherwise we should add it to the end of the sitemap
  await utils.later(300);
  if(!data.crawl_status) {
    data.crawl_status = 'finished';
  }
  if(is_external_link(url)) {
    var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all_external.csv');
    var found_link = false;
    for(let possible_match of sitemap_urls) {
      if(possible_match.url == url) {
        possible_match.status = data.status;
        possible_match.crawl_status = data.crawl_status;
        found_link = true;
      }
    }
    if(!found_link) {
      sitemap_urls.push({url:url,status:data.status,crawl_status:data.crawl_status});
    }
    var super_site_map_csv = await utils.jsonToCsv(sitemap_urls);
    return await utils.dataToFile(super_site_map_csv,'./sitemap_all_external.csv');
  } else {
    var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all.csv');
    if(data.status) {
      var found_link = false;
      for(let possible_match of sitemap_urls) {
        if(possible_match.url == url) {
          possible_match.status = data.status;
          possible_match.crawl_status = data.crawl_status;
          found_link = true;
        }
      }
      if(!found_link) {
        sitemap_urls.push({url:url,status:data.status,crawl_status:data.crawl_status});
        add_url_to_child_sitemap(url);
      } else {
        await modify_url_in_child_sitemap(url,data.status,data.crawl_status);
      }
    } else {
      sitemap_urls.push({url:url,status:'',crawl_status:''});
      add_url_to_child_sitemap(url);
    }
    var super_site_map_csv = await utils.jsonToCsv(sitemap_urls);
    return await utils.dataToFile(super_site_map_csv,'./sitemap_all.csv');
  } 
}



const check_for_url_in_sitemap = async function(url, external){
  var csv_file = external ? 'sitemap_all_external.csv' : 'sitemap_all.csv';
  var sitemap_urls = await utils.readCsvFileAsJson(csv_file);
  var found_link = false;
  // console.log('checking for ', url, in 'csv_file');

  for(let possible_match of sitemap_urls) {
    if(possible_match.url == url) {
      found_link = true;
    }
  }
  // console.log('found?', found_link);
  return found_link;
}

const add_url_to_sitemaps = async function(url, data) {
  var internal = is_internal_link(url);
  let csv_file = internal ? 'sitemap_all.csv' : 'sitemap_all_external.csv';

  let found = await check_for_url_in_sitemap(url, !internal);
  if(!found) {
    var row = Object.assign({url:url}, data);

    //add to the big sheet
    await utils.appendRowToCSV(row, csv_file);
    //add to the child sheet
    // await add_url_to_child_sitemap(url);
  }
  return true;
}

const is_internal_link = function (url) {
  var domain = utils.determineHost(url);
  return rootDomains.indexOf(domain) > -1;
}

const is_external_link = function(url) {
  return !is_internal_link(url);
}

const maybe_mark_completed = async function(url) {
  let existingStats = await scrape.getCompleteExistingStats(url);
  if(existingStats.time) {
    console.log(url, 'has already been completely crawled, marking complete');
    await modify_url_in_sitemap(url,{status:200})
  } else {
    console.log(url, 'not completely crawled yet');
  }
  return await utils.later(40);
}


const crawl_neon_url = async function(url, glitch_server) {
  var starttime = new Date().getTime();

  var meta = await utils.getMetaInfo(url);
  var status = parseInt(meta.status);

  if(status == 200) {
    //now we can scrape for external links, and add internal links not listed to the super_sitemap
    let page_info = await scrape.getInfo({url:url,skip_marking:true,prefetched_meta:meta,neon:true,glitch_server:glitch_server});
    let crawl_status = 'partial';
    if(parseInt(page_info.http_status) != 200) {
      crawl_status = 'finished';
    } else {
      if(page_info.screen_shots.desktop 
        && page_info.screen_shots.mobile) {
        crawl_status = 'finished';
      }
    }
    let all_links_on_page = page_info.links;
    // console.log(page_info);
    // await utils.later(5000);
    // let all_links_on_page = [];
    for(let link_from_page of all_links_on_page) {
      if(is_internal_link(link_from_page)) {
        var in_sitemap_already = await check_for_url_in_sitemap(link_from_page);
        if(!in_sitemap_already) {
          console.log('found', link_from_page, 'for sitemap');
          await add_url_to_sitemaps(link_from_page);
          
        }
      } else {
        var in_sitemap_already = await check_for_url_in_sitemap(link_from_page, true);
        if(!in_sitemap_already) {
          let external_meta = await utils.getMetaInfo(link_from_page);
          let external_status = external_meta.status;
          await add_url_to_sitemaps(link_from_page,{status:external_status});
          console.log('added', link_from_page, 'to external');
        }
      }
    }
    if(page_info.final_url == page_info.initial_url) {
      await update_csv_row_by_url(url, {crawl_status:crawl_status});
    } else {
      // await modify_url_in_sitemap(url,'3XX');
    }
  } else {
    //write the status of the url into the sitemap
    await update_csv_row_by_url(url, {status:status});
  }

  
  let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
  console.log(url + 'crawled and scraped | ' + elapsed+' s ');
  return await utils.later(10);
  
}

const findNextCrawlableUrl = async function(randomize, filter) {
  await utils.later(100);
  var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all.csv');
  var found_link = false;
  var stats = {total:sitemap_urls.length,fin:0};
  if(randomize) {
    shuffle(sitemap_urls);
  }
  for(let possible_link of sitemap_urls) {
    
    if(!found_link && possible_link.crawl_status != 'finished') {
      if(filter) {
        if(possible_link.url.indexOf(filter) > -1) {
          found_link = possible_link.url;
        }
      } else {
        found_link = possible_link.url;
      }
    }
    if(possible_link.crawl_status == 'finished') {
      stats.fin++;
    }
  }
  console.log(stats.fin, 'of',stats.total,'links finished', new Date().toString());
  return found_link;
}

const crawl_super_sitemap = async function()  {

  let filter = args.filter || args.f;
  let glitch_server = args.g || args.gs || args.glitch_server;

  var starttime = new Date().getTime();
  async function checkForAnotherURLToScrape() {
    await utils.wait(10);
    let next_url = await findNextCrawlableUrl(true,filter);
    console.log('next | ', next_url);
    if(next_url) {
      await crawl_neon_url(next_url,glitch_server);
      await checkForAnotherURLToScrape();
    } else {
      let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
      console.log('all urls checked or scraped | ' + elapsed+' s | ');
    }
  }

  await checkForAnotherURLToScrape(args);
}

const mark_all_completions = async function() {
  let filter = args.filter || args.f;
  let glitch_server = args.g || args.gs || args.glitch_server;

  var starttime = new Date().getTime();
  async function checkForAnotherURLToScrape() {
    await utils.wait(10);
    let next_url = await findNextCrawlableUrl(true,filter);
    console.log('next | ', next_url);
    if(next_url) {
      await maybe_mark_completed(next_url);
      await checkForAnotherURLToScrape();
    } else {
      let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
      console.log('all urls checked | ' + elapsed+' s | ');
    }
  }

  await checkForAnotherURLToScrape(args);
}
const not_file = function(url) {
  
  var is_file = url.indexOf('.png')>-1 ||
    url.indexOf('.pdf')>-1||
    url.indexOf('.jpg')>-1||
    url.indexOf('.gif')>-1||
    url.indexOf('.svg')>-1||
    url.indexOf('.jpeg')>-1;

  console.log('url we are looking at', url, is_file);
  return !is_file;
}

const remove_duplicates_from_sitemap = async function() {
  let url = args.url || args.u || false;
  let csv_filename = 'sitemap_all.csv'
  if(url) {

    csv_filename = url+'/sitemap.csv';
  }
  var sitemap_urls = await utils.readCsvFileAsJson(csv_filename);
  console.log(sitemap_urls.length, 'initial urls');
  sitemap_urls = _.filter(sitemap_urls,function(row){
    let main_url = row.url.split('://')[1] + '/';
    main_url = main_url.split(' ').join('');
    main_url = main_url.replace('//','/');
    main_url = 'https://' + main_url;
    main_url = main_url.replace(/"/g, '');
    main_url = main_url.trim();
    main_url = main_url.replace('://neoncrm','://www.neoncrm');
    main_url = main_url.split('#')[0];
    main_url = main_url.split('?')[0];
    row.url = main_url;


    if(not_file(row.url)&&row.url.length) {
      return row;
    }
  });

  sitemap_urls = _.sortBy(sitemap_urls, 'url');
  
  sitemap_urls = _.uniqBy(sitemap_urls, 'url');

  console.log(sitemap_urls.length, 'final urls');
  console.log(sitemap_urls[0]);
  var site_csv = await utils.jsonToCsv(sitemap_urls);
  return await utils.dataToFile(site_csv,'./'+csv_filename);
}

const turn_sitemap_into_json = async function() {
  var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all.csv');
  return await utils.dataToFile(JSON.stringify(sitemap_urls),'./sitemap_all.json');
}

/* bringing data to google */
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

const find_next_unchecked_url = async function() {
  await utils.later(100);
  var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all.csv');
  var found_link = false;

  for(let possible_link of sitemap_urls) {
    if(!found_link && !possible_link.status) {
      found_link = possible_link.url;
    }
  }
  return found_link;
}

//step 2
// check the https status
// record a non 200
const check_url_status = async function(url) {
  var status = await utils.getHTTPStatus(url);
  return status.status;
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
const json_to_google = async function(json, data_sheet) {

  let base ='https://nobleintentstudio.com/neon-sitemaps/' + json.full_host + json.path;
  let status = json.http_status;
  let data_to_write = {};
  if(json.sheet_status) {
    data_to_write = {
      url: json.url,
      host: json.full_host,
      path: json.path,
      status: json.status,
      sheet_status: json.sheet_status,
      timestamp: json.timestamp
    };
  } else {

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

    data_to_write = {
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


  }

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


const add_url_stats_to_google = async function(url,data_sheet) {
  let status = await check_url_status(url);

  let full_host = await utils.determineHost(url);
  let path = await utils.determinePath(url);
  let json = {url:url, full_host:full_host, path:path, status:status};
  let url_stats;
  if(status == 200 || status == '200') {
    url_stats = await find_url_json(url);
    json = Object.assign(json, url_stats);
  } else {
    json.status = status;
    json.timestamp = (new Date().toString());
    console.log('*************warning*************');
    console.log('non 200 status code for',url,status);
    console.log('*******************************');
  }



  if(url_stats) {
    let result = await json_to_google(json, data_sheet);
    await record_completion(url, result.sheet_status);
    return result.sheet_status;
  } else {
    json.sheet_status = 'non_200'
    await json_to_google(json, data_sheet);
    await record_completion(url, 'errored');
    return 'errored';
  }
}

const record_sitemap_data_in_google = async function()  {

  let doc = new GoogleSpreadsheet('1ZiwEAAqVtHiuBUvEZ9WerK2WATXt4iVcMyS1AigGVKw');
  await doc.useServiceAccountAuth(google_creds); 
  await doc.loadInfo(); // loads document properties and worksheets
  const data_sheet = doc.sheetsByIndex[0];

  let starttime = new Date().getTime();
  async function checkForAnotherURLToScrape() {
    let starttime2 = new Date().getTime();
    await utils.wait(1000);
    let next_url = await find_next_unsynced_url();
    console.log('next url  |', next_url);
    if(next_url) {
      let sheet_status = await add_url_stats_to_google(next_url, data_sheet);
      if(sheet_status == 'incomplete') {
        await crawl_neon_url(next_url);
        await add_url_stats_to_google(next_url, data_sheet);
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

const sitemap_checker = async function() {
  let starttime = new Date().getTime();
  async function checkForAnotherURLToScrape() {
    let starttime2 = new Date().getTime();
    await utils.wait(500);
    let next_url = await find_next_unchecked_url();
    console.log('next url  |', next_url);
    if(next_url) {
      let status = await utils.getHTTPStatus(next_url);
      let http_status = status.status;
      let external = is_external_link(next_url);
      let csv_file = external ? 'sitemap_all_external.csv' : 'sitemap_all.csv';

      if(http_status == 200) {
        await crawl_neon_url(next_url);
      }

      await update_csv_row_by_url(next_url,{status:http_status},csv_file)

      if(status.location) {
        if(status.location[0] != 'h') {
          //probably internal
          if(status.location[0] == '/') { //now it must be relative
           status.location = utilsdetermineHost(next_url) +status.location; //as long as it met the filter earlier, it must be on both same sub and root domain
          }
        }

        await add_url_to_sitemaps(status.location);
      }
      
      await crawl_neon_url(next_url);

      checkForAnotherURLToScrape();
    } else {
      let elapsed = utils.computeSecondsDiff(starttime,new Date().getTime());
      console.log('all urls recorded | ' + elapsed+' s');
    }
  }

  await checkForAnotherURLToScrape();
}

const start = async function() {
  let task = args.task || args.t || 'scrape';
  if(task == 'scrape') {
    await crawl_super_sitemap();
  } else if(task == 'mark') {
    await mark_all_completions();
  } else if(task == 'sitemap') {
    await combine_neon_sitemaps();
  } else if(task == 'new_sitemaps') {
    await get_neon_sitemaps();
  } else if(task == 'dedupe') {
    await remove_duplicates_from_sitemap();
  } else if(task=='tojson') {
    await turn_sitemap_into_json();
  } else if(task == 'to_google') {
    await record_sitemap_data_in_google();
  } else if(task == 'sitemap_checker') {
    await sitemap_checker();
  } else if(task=='reset_crawl_status') {
    await remove_column_values('crawl_status');
  } else if(task=='reset_sheet_status') {
    await remove_column_values('sheet_status');
  }

  // get_neon_sitemaps();
  // crawl_neon_sites();
  // 
  
  console.log('--------waited----------------');
}

start();