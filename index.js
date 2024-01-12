
const fetch = require('node-fetch');
const csv = require('csvtojson');
const settings = require('./settings');

const COUCHDB_URL = settings.couchUrl;
const CSV_FILE = 'malawi-11-01-2024';

const docs = [];
const requestOptions = {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
};

const muterStatus = ['Dead', 'TransferOut'];

/**
 * 
 * @param {string} dateStr 
 */
function useDateStrToLocal(dateStr) {
  const asArr = dateStr.split(' ');
  // const asArr = dateStr.replace('/', '-').split('-'); // the format changes from time to time in their csv
  const localeDate = `${asArr[2]}-${asArr[1]}-${asArr[0]}`; // YYYY-MM-DD
  return localeDate;
}

/**
 * 
 * @param {string[]} nationalIDs 
 */
async function couchFind(nationalIDs) {
  const idObjects = nationalIDs.map(id => ({ "national_id": id }));
  const query = {
    'selector': {
      '$or': idObjects
    }
  };

  const options = {
    ...requestOptions,
    body: JSON.stringify(query)
  };
  console.log(JSON.stringify(query));
  try {
    const response = await fetch(`${COUCHDB_URL}/_find`, options);
    if (!response.ok) throw response.error;
    const data = await response.json().catch(e => { throw Error(e); });
    console.log(data.docs.length, '@#@#@@ contacts from db');
    return data.docs;
  } catch (e) {
    throw Error('fetch records failed', e);
  }
}

async function couchBulkUpdate() {
  if (!(Array.isArray(docs) && docs.length)) {
    throw fetch.FetchError('cannot update with empty docs');
  }
  try {
    const bulkUpdateEndpoint = `${COUCHDB_URL}/_bulk_docs`;
    const options = { ...requestOptions, body: JSON.stringify({ docs }) };
    const response = await fetch(bulkUpdateEndpoint, options);
    const data = await response.json().catch(e => console.error(e));
    return data;
  } catch (e) {
    throw Error('update records failed', e);
  }
}

async function getReportsFromCSV() {
  const csvBasePath = '/data/';
  try {
    const patches = await csv()
      .fromFile(`./data/${CSV_FILE}.csv`);
    return patches
      .filter(p => (p.next_appoint_date && p.visit_date))
      .map(p => {
        console.log(p);
        return ({
          visit_date: new Date(useDateStrToLocal(p.visit_date)).toISOString(),
          next_visit: new Date(useDateStrToLocal(p.next_appoint_date)).toISOString(),
          national_id: p.National_patient_Id,
          art_status: p.ARToutcome,
          art_status_change_date: new Date(useDateStrToLocal(p.OutcomeDate)).toISOString()
        });
      });
  } catch (e) {
    console.error(e);
  }
}

async function getRapidproState(contacts) {
  const keys = contacts.map(contact => ({ id: `${contact._id}-rapidpro` }));
  const options = {
    ...requestOptions,
    body: JSON.stringify({ docs: keys })
  };
  try {
    const response = await fetch(`${COUCHDB_URL}/_bulk_get`, options);
    const data = await response.json();
    console.log('#$@ contacts rapidpro from db', data.results.length);
    const d = data.results
      .map(d => (d.docs[0]))
      .filter(d => {
        // in rare cases where contact does not
        // have a rapidpro object
        if (!d.ok) {
          console.warn('Missing rapidpro state object for contact', d);
          return false;
        }
        return true;
      })
      .map(d => (d.ok));
    return d;
  } catch (e) {
    throw fetch.FetchError(e);
  }
}
/**
 * 
 * @param {array} contacts 
 * @param {array} visits 
 */
function updateVisits(contacts, rapidproStates, visits) {
  contacts
    .filter(contact => {
      const visit = visits.find(visit => contact.national_id === visit.national_id);
      if (!visit) {
        console.info(contact);
      }
      return visit;
    })
    .forEach(contact => {
      const visit = visits.find(visit => contact.national_id === visit.national_id);
      const rpState = rapidproStates.find(rps => (rps._id === `${contact._id}-rapidpro`));

      const visit_date = new Date(rpState.visit_date).getTime() >= new Date(visit.next_visit).getTime() ? rpState.visit_date : visit.next_visit;

      if (!contact.rapidpro) {
        contact.rapidpro = {};
      }
      contact.rapidpro.visit_date = visit_date;
      contact.art_status = visit.art_status;
      contact.art_status_change_date = visit.art_status_change_date;
      if (muterStatus.includes(visit.art_status)) {
        contact.muted = true;
        contact.rapidpro.optout = true;
        contact.rapidpro.reminders = false;
        contact.rapidpro.adherence = false;
      }

      if (contact.last_visit) {
        const visit_date_diff = (new Date(visit.visit_date).getSeconds() - new Date(contact.last_visit).getSeconds()) / (60 * 60 * 24);
        if (visit_date_diff > 14) {
          contact.visit_count = Boolean(contact.visit_count) ? (contact.visit_count + 1) : 1;
        }
      }
      contact.last_visit = visit.visit_date;
      docs.push(contact);
      if (rpState) {
        const rapidproDoc = {
          ...rpState,

          visit_date
        };
        docs.push(rapidproDoc);
      }
    });
}


(async function init() {
  try {
    const nationalIDs = [];
    let contacts = [];
    console.info('>>>>> started: getting appointments from csv');
    const appointments = await getReportsFromCSV();
    console.info('>>>>> completed: getting appointments from csv', appointments.length);

    appointments.forEach(ap => nationalIDs.push(ap.national_id));
    const al = nationalIDs.length;

    console.info('>>>>> started: getting contacts from db for', nationalIDs.length);
    while (nationalIDs.length) {
      const cs = await couchFind(nationalIDs.splice(0, 20));
      contacts = [...contacts, ...cs];
    }
    console.info('>>>>> completed: getting contacts from db', contacts.length);

    console.info('>>>>> started: fetching rapidpro state from db');
    const rapidproStates = await getRapidproState(contacts);
    console.info('>>>>> completed: fetching rapidpro state from db');

    console.info('>>>>> started: updating contacts');
    updateVisits(contacts, rapidproStates, appointments);
    console.info('>>>>> completed: updating contacts');

    console.info('>>>>> started: updating contacts on db');
    const updatedDocs = await couchBulkUpdate();
    console.info('>>>>> completed: updating contacts on db');

    console.info('>>>>> completed!', JSON.stringify(updatedDocs));
  } catch (e) {
    throw Error(e);
  }

})()

