require('dotenv').config();
var myArgs = require('optimist').argv;
const axios = require('axios');
const {
  filterResponse,
  getDates,
  letterGenerator,
  nextKeyGenerator,
  candidateChooser,
  fetchIntegrityGenerator
} = require('./helpers');
const url = 'mongodb://localhost:27017/';
const dbConfig = { useNewUrlParser: true };
const mongodb = require('mongodb');
const client = mongodb.MongoClient;
const nextKey = nextKeyGenerator(myArgs.keys);
const CANDIDATE = candidateChooser(myArgs.id);
let KEY = nextKey();
const IDS = ['P60007671', 'P60008075', 'P60008885'];
const END_DATE = new Date(2016, 06, 12); // Bernie drops out

// query params for API and
let min_date, max_date, formattedDate;
let fetchedSoFar = 0;
let count = 0;
let nextLetter;
let l_idx = '';
let l_date = '';

// CONNECT TO MONGO

client.connect(url, dbConfig, async (err, mongoClient) => {
  if (err) throw err;
  const db = mongoClient.db('lib_rad');

  await startCandidate(db, CANDIDATE);

  // for (let i = 0; i < IDS.length; i++) {
  //   const candidate = candidateChooser(IDS[i]);
  //   console.log(candidate);
  //   await startCandidate(db, candidate);
  // }

  console.log('Done!!!');
  mongoClient.close();
});

async function startCandidate(db, candidate) {
  ({ min_date, max_date, formattedDate } = getDates(candidate.date));

  return new Promise(async res => {
    while (min_date < END_DATE) {
      console.log(' ');
      console.log(
        `STARTING NEW FETCH ROUND: ${candidate.name} ${formattedDate}.....`
      );
      console.log(' ');

      // reset nextLetter function
      nextLetter = letterGenerator();
      // start recursive fetch process
      const fetchSuccess = await fetchContributions(db, candidate);
      // if fetch failed, don't advance dates
      if (!fetchSuccess) continue;
      ({ min_date, max_date, formattedDate } = getDates(min_date));
    }
    res();
  });
}

function fetchContributions(db, candidate) {
  let contributions = [];
  let didPaginationFail = fetchIntegrityGenerator();
  fetchedSoFar = 0;

  return new Promise((resolve, reject) => {
    // each fetchNext call gets up to 100 more results and
    // adds them to the contributions array
    fetchNext();

    async function fetchNext() {
      let URL = `https://api.open.fec.gov/v1/schedules/schedule_a/?sort_null_only=false&max_date=${max_date.toISOString()}&two_year_transaction_period=2016&api_key=${KEY}&min_date=${min_date.toISOString()}&committee_id=${
        candidate.cmte_id
      }&sort=contribution_receipt_date&sort_hide_null=false&per_page=100`;

      // these keep track of pagination
      // only need to be appended after the first fetch
      if (l_idx) {
        URL += `&last_index=${l_idx}&last_contribution_receipt_date=${l_date}`;
      }

      // else fetch next batch
      axios
        .get(URL)
        .then(async response => {
          const filtered = filterResponse(response);
          fetchedSoFar += filtered.length;
          contributions.push(...filtered);
          const pageData = response.data.pagination;
          count = pageData.count;

          // console.log(URL);

          // checks if the fetch pagination is working correctly
          // sometimes it reports a larger total count than what it sends back
          if (didPaginationFail(contributions.length)) {
            console.log('======================');
            console.log(
              `Bad fetch count: ${count} -- aborting & saving ${
                contributions.length
              } contributions on ${formattedDate}`
            );
            console.log('');

            initSave(db, contributions, formattedDate, candidate, count);
            resolve(true);
          }

          // check to see if any results returned
          if (count === 0 || !contributions.length) {
            console.log('======================');
            console.log(`no contributions on ${formattedDate}...`);
            console.log('======================');
            resolve(true);
          }

          // check to see if this day is already successfully saved to db
          else if (contributions.length < 100) {
            if (await checkIfAllSaved(db, formattedDate, count, candidate)) {
              resolve(true);
            }
          }

          // if done fetching, save to DB
          else if (count && fetchedSoFar >= count) {
            initSave(db, contributions, formattedDate, candidate, count);
            resolve(true);
            return;
          }

          // save 10k if array is getting too big for its britches & continue
          if (contributions.length === 10000) {
            initSave(db, contributions, formattedDate, candidate, count);
            contributions = [];

            // resets didPaginationFail function
            didPaginationFail = fetchIntegrityGenerator();
          }

          // prepare for next fetch
          if (pageData.last_indexes) {
            l_idx = pageData.last_indexes.last_index;
            l_date = pageData.last_indexes.last_contribution_receipt_date;
          }

          console.log(`-------${formattedDate}--------`);
          console.log('count: ', count);
          console.log('fetchedSoFar: ', fetchedSoFar);
          console.log('l_idx: ', l_idx, '  l_date: ', l_date);

          fetchNext();
        })
        .catch(err => {
          if (err.response && err.response.status == 429) {
            KEY = nextKey();

            fetchNext();
          } else {
            console.log(err);
          }
        });
    }
  });
}

async function initSave() {
  try {
    await save(...arguments);
    return;
  } catch (err) {
    console.log(err);
  }
}

function save(db, contributions, date, candidate, count) {
  const letter = nextLetter();

  return new Promise((res, rej) => {
    const _id = `${date + letter}`;
    console.log('======================');
    console.log(`saving ${_id} contributions...`);

    const object = {
      _id,
      count,
      contributions
    };

    const doc = db.collection(candidate.name);
    doc
      .updateOne({ _id: _id }, { $set: object }, { upsert: true })
      .then(() => {
        console.log(`${_id} success!`);
        console.log('======================');
        res(true);
      })
      .catch(rej);
  });
}

function checkIfAllSaved(db, dateId, count, candidate) {
  return new Promise((resolve, rej) => {
    const doc = db.collection(candidate.name);
    doc
      .aggregate(
        { $match: { _id: dateId } },
        { $unwind: '$contributions' },
        { $group: { count: { $sum: 1 } } }
      )
      .toArray((err, saved) => {
        if (err) rej(err);
        if (saved.length === count) {
          console.log('======================');
          console.log(`${date} already saved to DB! :D`);
          console.log('======================');
          resolve(true);
        }
        resolve(false);
      });
  });
}
