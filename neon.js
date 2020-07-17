//takes a sitemap url
//and finds sub sitemaps and compiles all links found inside of <loc> xml


//$: node _sitemap.js -u https://www.neoncrm.com/sitemap_index.xml

const myArgs = process.argv.slice(2);
const urlToCheck = myArgs[0];
const fs = require('fs');

var _ = require('lodash');
const shuffle = require('shuffle-array');


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
  'ww2.neonone.com',
  'ww2.rallybound.com',
  'ww2.neoncrm.com'
];
let rootUrls = [
  'https://neonone.com',
  'https://www.arts-people.com',
  'https://www.neoncrm.com',
  'https://rallybound.com',
  'https://www.civicore.com',
  'https://ww2.neonone.com',
  'https://ww2.rallybound.com',
  'https://ww2.neoncrm.com'
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
        // line.crawl_status = '';
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
  sitemap_urls.push({url:url,status:''});
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

const modify_url_in_sitemap = async function(url, status, external, crawl_status) {
  //if external, then we should add to the external list
  //if status, then we need to update the row
  //otherwise we should add it to the end of the sitemap
  await utils.later(300);
  if(!crawl_status) {
    crawl_status = 'finished';
  }
  if(external) {
    var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all_external.csv');
    if(sitemap_urls.length < total_sitemap_link_count) {
      //not good! maybe it died while reading? GTFO!
      console.log('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
      console.log('sitemap_urls got smaller! From',total_sitemap_link_count, 'to',sitemap_urls.length);
      return await utils.later(10);
    } else {
      total_sitemap_link_count = sitemap_urls.length;
    }
    var found_link = false;
    for(let possible_match of sitemap_urls) {
      if(possible_match.url == url) {
        possible_match.status = status;
        possible_match.crawl_status = crawl_status;
        found_link = true;
      }
    }
    if(!found_link) {
      sitemap_urls.push({url:url,status:status,crawl_status:crawl_status});
    }
    var super_site_map_csv = await utils.jsonToCsv(sitemap_urls);
    return await utils.dataToFile(super_site_map_csv,'./sitemap_all_external.csv');
  } else {
    var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all.csv');
    if(status) {
      var found_link = false;
      for(let possible_match of sitemap_urls) {
        if(possible_match.url == url) {
          possible_match.status = status;
          possible_match.crawl_status = crawl_status;
          found_link = true;
        }
      }
      if(!found_link) {
        sitemap_urls.push({url:url,status:status,crawl_status:crawl_status});
        add_url_to_child_sitemap(url);
      } else {
        await modify_url_in_child_sitemap(url,status,crawl_status);
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

const is_internal_link = function (url) {
  var domain = utils.determineHost(url);
  // console.log('is', url,'internal?', domain, rootDomains.indexOf(domain));
  return rootDomains.indexOf(domain) > -1;
}

const maybe_mark_completed = async function(url) {
  let existingStats = await scrape.getCompleteExistingStats(url);
  if(existingStats.time) {
    console.log(url, 'has already been completely crawled, marking complete');
    await modify_url_in_sitemap(url,200)
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
            await modify_url_in_sitemap(link_from_page);
            console.log('added', link_from_page, 'to sitemap');
          }
        } else {
          var in_sitemap_already = await check_for_url_in_sitemap(link_from_page, true);
          if(!in_sitemap_already) {
            let external_meta = await utils.getMetaInfo(link_from_page);
            let external_status = external_meta.status;
            await modify_url_in_sitemap(link_from_page,external_status,true);
            console.log('added', link_from_page, 'to external');
          }
        }
      }
      if(page_info.final_url == page_info.initial_url) {
        await modify_url_in_sitemap(url, 200,false,crawl_status);
      } else {
        await modify_url_in_sitemap(url,'3XX');
      }
    } else {
      //write the status of the url into the sitemap
      await modify_url_in_sitemap(url, status);
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

const remove_duplicates_from_sitemap = async function() {
  let url = args.url || args.u || false;
  let csv_filename = 'sitemap_all.csv'
  if(url) {
    csv_filename = url+'/sitemap.csv';
  }
  var sitemap_urls = await utils.readCsvFileAsJson(csv_filename);
  
  sitemap_urls = _.sortBy(sitemap_urls, 'url');
  console.log(sitemap_urls.length, 'initial urls');
  sitemap_urls = _.uniqBy(sitemap_urls, 'url');
  console.log(sitemap_urls.length, 'final urls');
  var site_csv = await utils.jsonToCsv(sitemap_urls);
  return await utils.dataToFile(site_csv,'./'+csv_filename);
  
  
  sitemap_urls.push({url:url,status:''});
  
}

const turn_sitemap_into_json = async function() {
  var sitemap_urls = await utils.readCsvFileAsJson('sitemap_all.csv');
  return await utils.dataToFile(JSON.stringify(sitemap_urls),'./sitemap_all.json');
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
  }

  // get_neon_sitemaps();
  // crawl_neon_sites();
  // 
  
  console.log('--------waited----------------');
}

start();