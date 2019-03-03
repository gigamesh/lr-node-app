const fs = require('fs');
const path = require('path');
require('dotenv').config();
const { KEY_1A, KEY_2A, KEY_3A, KEY_1B, KEY_2B, KEY_3B } = process.env;

function filterResponse(res) {
  return res.data.results.map(o => ({
    line_number: o.line_number,
    report_type: o.report_type,
    entity_type_desc: o.entity_type_desc,
    contribution_receipt_amount: o.contribution_receipt_amount,
    donor_committee_name: o.donor_committee_name,
    receipt_type: o.receipt_type,
    contributor_zip: o.contributor_zip,
    contributor_aggregate_ytd: o.contributor_aggregate_ytd,
    contributor: o.contributor,
    contributor_occupation: o.contributor_occupation,
    transaction_id: o.transaction_id,
    sub_id: o.sub_id,
    contributor_last_name: o.contributor_last_name,
    contributor_first_name: o.contributor_first_name,
    contributor_street_1: o.contributor_street_1,
    conduit_committee_street2: o.conduit_committee_street2,
    conduit_committee_city: o.conduit_committee_city,
    contributor_state: o.contributor_state,
    contributor_employer: o.contributor_employer,
    file_number: o.file_number,
    contribution_receipt_date: o.contribution_receipt_date
  }));
}

function fetchIntegrityGenerator() {
  let fetches = 0;
  let internalTally = 0;
  return contribLength => {
    console.log(
      'fetches: ',
      fetches,
      ' internalTally: ',
      internalTally,
      ' contribLength: ',
      contribLength
    );
    ++fetches;
    if (fetches === 1) {
      internalTally = contribLength;
    }
    return fetches > 1 && internalTally === contribLength;
  };
}

function getDates(startDate) {
  // advance startDate by 1
  startDate.setDate(startDate.getDate() + 1);
  let temp = new Date(startDate.getTime());

  // advance next day by 1
  temp.setDate(temp.getDate() + 1);
  let min_date = startDate;
  let max_date = temp;

  let month = (max_date.getMonth() + 1).toString().padStart(2, '0');
  let day = max_date
    .getDate()
    .toString()
    .padStart(2, '0');
  let formattedDate = `${max_date.getFullYear()}-${month}-${day}`;

  return {
    min_date,
    max_date,
    formattedDate
  };
}

function saveToJSON(array) {
  fs.writeFile(
    path.join(__dirname, 'temp.json'),
    JSON.stringify(array),
    err => {
      if (err) throw err;
      console.log('File written to...');
    }
  );
}

function createCandidates(mongoClient) {
  const db = mongoClient.db('lib_rad');

  DEMS.forEach(candidate => {
    candidate._id = candidate.candidate_id;

    const doc = db.collection(candidate.name);
    doc
      .insertOne(candidate)
      .then(item => {
        console.log(`${candidate.name} inserted!`);
      })
      .catch(console.log);
  });

  mongoClient.close();
}

function letterGenerator() {
  const letters = [
    'A',
    'B',
    'C',
    'D',
    'E',
    'F',
    'G',
    'H',
    'I',
    'J',
    'K',
    'L',
    'M',
    'N',
    'O',
    'P',
    'Q',
    'R',
    'S',
    'T',
    'U',
    'V'
  ];
  let i = 0;
  return () => {
    const letter = '-' + letters[i];
    i++;
    return letter;
  };
}

function nextKeyGenerator(keyGroup) {
  if (!keyGroup) throw 'No keygroup supplied!';
  const keysA = [KEY_1A, KEY_2A, KEY_3A];
  const keysB = [KEY_1B, KEY_2B, KEY_3B];
  const keys = keyGroup === 'A' ? keysA : keysB;

  let tick = -1;
  return () => {
    tick++;
    const i = tick % keys.length;
    console.log('NEW API KEY: ' + keys[i]);
    return keys[i];
  };
}

function candidateChooser(id) {
  const DEMS = [
    {
      name: 'Bernie Sanders',
      candidate_id: 'P60007168',
      cmte_id: 'C00577130',
      party: 'DEM',
      date: new Date(2016, 01, 28)
    },
    {
      name: 'Hillary Clinton',
      candidate_id: 'P00003392',
      cmte_id: 'C00575795',
      party: 'DEM',
      date: new Date(2016, 04, 04)
    },
    {
      name: 'Martin OMalley',
      candidate_id: 'P60007671',
      cmte_id: 'C00578658',
      party: 'DEM',
      date: new Date(2015, 04, 13)
    },
    {
      name: 'Lincoln Chafee',
      candidate_id: 'P60008075',
      cmte_id: 'C00579706',
      party: 'DEM',
      date: new Date(2015, 04, 10)
    },
    {
      name: 'Jim Webb',
      candidate_id: 'P60008885',
      cmte_id: 'C00581215',
      party: 'DEM',
      date: new Date(2015, 04, 10)
    }
  ];
  return DEMS.find(o => id === o.candidate_id);
}

module.exports = {
  filterResponse,
  createCandidates,
  getDates,
  removeDuplicates,
  saveToJSON,
  letterGenerator,
  nextKeyGenerator,
  candidateChooser,
  fetchIntegrityGenerator
};
