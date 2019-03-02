require('dotenv').config();
const KEY = process.env.KEY1;
const axios = require('axios');
const {
  filterResponse,
  getDates,
  removeDuplicates,
  createCandidates
} = require('./helpers');
const url = 'mongodb://localhost:27017/';
const dbConfig = { useNewUrlParser: true };
const mongodb = require('mongodb');
const client = mongodb.MongoClient;

const CANDIDATE = 'Bernie Sanders';
// Hillary announcement:  new Date(2015, 03, 12);

const START_DATE = new Date(2015, 09, 12);
const END_DATE = new Date(2016, 06, 12); // Bernie drops out

// query params for API and
let { min_date, max_date, formattedDate } = getDates(START_DATE);

// CONNECT TO MONGO

client.connect(url, dbConfig, async (err, mongoClient) => {
  if (err) throw err;
  const db = mongoClient.db('lib_rad');

  while (min_date < END_DATE) {
    console.log(`ABOUT TO START A NEW FETCH ROUND: ${formattedDate}.....`);

    // start recursive fetch process
    const fetchSuccess = await fetchContributions();

    // if fetch failed, don't advance dates
    if (!fetchSuccess) continue;
    ({ min_date, max_date, formattedDate } = getDates(min_date));
    console.log('next date: ', formattedDate);
  }

  function fetchContributions() {
    let contributions = [];

    return new Promise(resolve => {
      // each fetchNext call gets up to 100 more results and
      // adds them to the contributions array
      fetchNext();

      async function fetchNext(count, l_idx, l_date) {
        let URL = `https://api.open.fec.gov/v1/schedules/schedule_a/?sort_null_only=false&max_date=${max_date.toISOString()}&two_year_transaction_period=2016&api_key=${KEY}&min_date=${min_date.toISOString()}&committee_id=C00577130&sort=contribution_receipt_date&sort_hide_null=false&per_page=100`;

        // these keep track of pagination
        // only need to be appended after the first fetch
        if (l_idx) {
          URL += `&last_index=${l_idx}&last_contribution_receipt_date=${l_date}`;
        }

        // if done fetching, return contributions
        if (count <= contributions.length) {
          // filter out any duplicates
          contributions = removeDuplicates(contributions, 'transaction_id');

          // check if the API messed up and if so, try that day again
          let ratio = contributions.length / count;
          if (ratio < 0.9) {
            console.log(
              'BAD RATIO: ',
              ratio,
              `trying ${formattedDate} again...`
            );
            resolve(false);
            return;
          }

          // ...else save to DB
          try {
            await save(db, contributions, formattedDate, count);
            resolve(true);
            return;
          } catch (err) {
            console.log(err);
          }

          // if not done fetching, keep fetching
        } else {
          axios.get(URL).then(async response => {
            contributions.push(...filterResponse(response));

            const pageData = response.data.pagination;

            // check to see if any results returned
            if (pageData.count === 0) {
              console.log('======================');
              console.log(`no contributions on ${formattedDate}...`);
              console.log('======================');
              resolve(true);
              return;
            }

            // check to see if this day is already successfully saved to db
            if (contributions.length < 101) {
              if (await checkIfAllSaved(db, formattedDate, pageData.count)) {
                resolve(true);
                return;
              }
            }

            // save 10k if array is getting too big for its britches
            if (contributions.length > 9000) {
              try {
                await save(db, contributions, formattedDate, count);
                contributions = [];
              } catch (err) {
                console.log(err);
              }
            }

            const {
              last_index: l_idx,
              last_contribution_receipt_date: l_date
            } = pageData.last_indexes;

            console.log('---------------');
            console.log('count: ', count);
            console.log('contributions.length: ', contributions.length);

            fetchNext(pageData.count, l_idx, l_date);
          });
        }
      }
    });
  }
  mongoClient.close();
});

function checkIfAllSaved(db, date, count) {
  return new Promise((resolve, rej) => {
    const doc = db.collection(CANDIDATE);
    doc
      .aggregate(
        { $match: { _id: date } },
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

function save(db, contributions, date, count) {
  const letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K'];
  const i = contributions.length / 10000;
  const letter = contributions.length >= 10000 ? letters[i] : '';

  return new Promise((res, rej) => {
    console.log('======================');
    console.log(`saving ${date}-${letter} contributions...`);
    console.log(`COUNT CHECK: ${count} === ${contributions.length}`);

    const object = {
      _id: `${date}-${letter}`,
      contributions
    };

    const doc = db.collection(CANDIDATE);
    doc
      .updateOne(
        { _id: `${date}-${letter}` },
        { $set: object },
        { upsert: true }
      )
      .then(() => {
        console.log(`${date}-${letter} success!`);
        console.log('======================');
        res(true);
      })
      .catch(rej);
  });
}

function initDocInDB(db, date) {
  return new Promise((res, rej) => {
    const doc = db.collection(CANDIDATE);
    const object = {
      _id: date,
      contributions: []
    };
    doc
      .updateOne({ _id: date }, { $set: object }, { upsert: true })
      .then(() => {
        console.log(`${date} initialized in DB`);
        console.log(' ');
        res(true);
      })
      .catch(rej);
  });
}
