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
const END_DATE = new Date(2016, 04, 03);

// query params for API and
let min_date, max_date, formattedDate;
let fetchedSoFar = 0;
let count = 0;
let nextLetter = letterGenerator();
let letter = nextLetter();
let l_idx = '';
let l_date = '';

// CONNECT TO MONGO

client.connect(url, dbConfig, async (err, mongoClient) => {
  if (err) throw err;
  const db = mongoClient.db('lib_rad');

  await startCandidate(db, CANDIDATE);

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

      // start recursive fetch process
      const fetchResult = await fetchContributions(db, candidate);

      // if fetch failed...
      if (!fetchResult.success) {
        console.log(`Retrying: ${formattedDate}-${letter}.....`);

        if (fetchResult.retry && fetchedSoFar > 10000) {
          await retryLastBatch(db, candidate);

          // else break out of loop without resetting dates & letter
        } else continue;
      }

      // reset nextLetter function, fetch counter, and move to next day
      nextLetter = letterGenerator();
      letter = nextLetter();
      fetchedSoFar = 0;
      l_idx = '';
      l_date = '';
      ({ min_date, max_date, formattedDate } = getDates(min_date));
    }
    res();
  });
}

function fetchContributions(db, candidate) {
  let contributions = [];
  let didPaginationFail = fetchIntegrityGenerator();

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
            contributions = [];
            console.log('pagination fail!');
            resolve({ success: false, retry: true });
            return;
          }

          // check to see if any results returned
          if (count === 0 || !contributions.length) {
            console.log('======================');
            console.log(`no contributions on ${formattedDate}...`);
            console.log('======================');
            resolve({ success: true });
            return;
          }

          // if done fetching, save to DB
          else if (count && fetchedSoFar >= count) {
            await initSave(db, contributions, candidate);
            resolve({ success: true });
            return;
          }

          // save 10k if array is getting too big for its britches & continue
          if (contributions.length === 10000) {
            await initSave(db, contributions, candidate);
            contributions = [];

            // resets didPaginationFail function
            didPaginationFail = fetchIntegrityGenerator();
          }

          // prepare for next fetch
          if (pageData.last_indexes) {
            l_idx = pageData.last_indexes.last_index;
            l_date = pageData.last_indexes.last_contribution_receipt_date;
          }

          console.log(
            `${formattedDate}-${letter} -- count: ${count}, fetchedSoFar: ${fetchedSoFar}`
          );

          fetchNext();
        })
        .catch(err => {
          if (err.response && err.response.status == 429) {
            KEY = nextKey();
            fetchNext();
          } else if (err.response && err.response.status == 500) {
            console.log('500 Server Error. continuing...');
            fetchNext();
          } else {
            console.log(err);
          }
        });
    }
  });
}

async function initSave() {
  return new Promise(async res => {
    try {
      await save(...arguments);
      res();
    } catch (err) {
      console.log(err);
    }
  });
}

function save(db, contributions, candidate) {
  return new Promise((res, rej) => {
    const _id = `${formattedDate}-${letter}`;
    console.log('======================');
    console.log(
      `saving ${_id}, ${
        contributions.length
      } contributions of count ${count}...`
    );

    const object = {
      _id,
      count,
      l_idx,
      l_date,
      contributions,
      savedSoFar: fetchedSoFar
    };

    const doc = db.collection(candidate.name);
    doc
      .updateOne({ _id: _id }, { $set: object }, { upsert: true })
      .then(() => {
        console.log(`${_id} success!`);
        console.log('======================');

        letter = nextLetter();
        res(true);
      })
      .catch(rej);
  });
}

function retryLastBatch(db, candidate) {
  return new Promise(res => {
    const prevLetter = String.fromCharCode(letter.charCodeAt() - 1);
    const _id = `${formattedDate}-${prevLetter}`;
    const doc = db.collection(candidate.name);

    console.log('Looking up previous saved batch: ', _id);

    // get l_idx and l_date from the previously saved 10k batch
    doc
      .findOne({ _id: _id }, { l_idx: 1, l_date, savedSoFar: 1 })
      .then(async result => {
        ({ l_idx, l_date, savedSoFar } = result);
        fetchedSoFar = savedSoFar;

        // start fetching again
        const fetchResult = await fetchContributions(db, candidate);

        // if fetch failed, try, try, try again...
        if (!fetchResult.success) {
          await retryLastBatch(db, candidate);
        } else {
          res();
        }
      });
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
