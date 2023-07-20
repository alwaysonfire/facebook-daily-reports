const bizSdk = require('facebook-nodejs-business-sdk');
const { cxnGetReportJsonData } = require('./service');

const cxnInit = async () => {
  console.log('Pulling from connexity');

  const cxnData = await cxnGetReportJsonData();
  console.log('cxnData', cxnData);
  const Content = bizSdk.Content;
  const CustomData = bizSdk.CustomData;
  const DeliveryCategory = bizSdk.DeliveryCategory;
  const EventRequest = bizSdk.EventRequest;
  const UserData = bizSdk.UserData;
  const ServerEvent = bizSdk.ServerEvent;

  let current_timestamp = Math.floor(new Date() / 1000);

  const access_token =
    'EAAEOhQp8hhYBAJPrq0ZCZAXqGZBNS4S5FzGkl5q2Ls5SGZCbBZAK4LHPpyjbUFVJsutZAj1NayUKSYSAJlzvOANppLJ7gcmtNaFV6McaT2rshqZChqtcWzSBdG0NAZCXAFdEKGm6UprnBiirvSyfCZAwOdqZCV7C5WkIb0Doqy3amJ8ZAUpjob2cGrD';
  const pixel_id = '3568073713440248';
  const api = bizSdk.FacebookAdsApi.init(access_token);

  let eventsData = [];

  for (const [index, item] of cxnData.entries()) {
    const userData = new UserData().setEmails([item.accountName]);

    const content = new Content().setId(item.stats.placementId);

    const customData = new CustomData()
      .setContents([content])
      .setCurrency('usd')
      .setValue(item.stats.sales);

    const serverEvent = new ServerEvent()
      .setEventName('Purchase')
      .setEventTime(current_timestamp)
      .setUserData(userData)
      .setCustomData(customData)
      .setActionSource('website');

    eventsData.push(serverEvent);

    // Check if eventsData reaches 1000 or it's the last iteration
    const isBatchReady =
      eventsData.length === 1000 || index === cxnData.length - 1;

    if (isBatchReady) {
      console.log('Processing batch:', eventsData.length);

      const eventRequest = new EventRequest(access_token, pixel_id).setEvents(
        eventsData
      );
      try {
        const res = await eventRequest.execute();
        console.log('res', res);
      } catch (e) {
        console.log('e', e);
      }

      // Reset eventsData for the next batch
      eventsData = [];
    }
  }

  process.exit(0);
};

cxnInit();