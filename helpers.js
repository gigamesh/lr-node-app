const fs = require('fs');
const path = require('path');

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

function removeDuplicates(originalArray, prop) {
  var newArray = [];
  var lookupObject = {};

  for (var i in originalArray) {
    lookupObject[originalArray[i][prop]] = originalArray[i];
  }

  for (i in lookupObject) {
    newArray.push(lookupObject[i]);
  }
  return newArray;
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

const DEMS = [
  {
    name: 'Bernie Sanders',
    candidate_id: 'P60007168',
    cmte_id: 'C00577130',
    party: 'DEM'
  },
  {
    name: 'Hillary Clinton',
    candidate_id: 'P00003392',
    cmte_id: 'C00575795',
    party: 'DEM'
  },
  {
    name: 'Martin OMalley',
    candidate_id: 'P60007671',
    cmte_id: 'C00578658',
    party: 'DEM'
  },
  {
    name: 'Lincoln Chafee',
    candidate_id: 'P60008075',
    cmte_id: 'C00579706',
    party: 'DEM'
  },
  {
    name: 'Jim Webb',
    candidate_id: 'P60008885',
    cmte_id: 'C00581215',
    party: 'DEM'
  }
];

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

module.exports = {
  filterResponse,
  createCandidates,
  getDates,
  removeDuplicates,
  saveToJSON
};
