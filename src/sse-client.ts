import {createEventSource} from 'eventsource-client'

const SSE_BASE_URL = process.env.SSE_BASE_URL || 'http://localhost:80';

const url = `${SSE_BASE_URL}/events`;

console.log(`Connecting to SSE server at ${url}...`);

const es = createEventSource({
  url,
  onMessage: ({data, event, id}) => {
    console.log('');
    console.log('Data: %s', data)
    console.log('Event ID: %s', id) // Note: can be undefined
    console.log('Event: %s', event) // Note: can be undefined
  }
  ,onConnect:() => {
    console.log('Connected to the event source server')
  }
  ,onDisconnect:() => {
    console.log('Disconnected from the event source server')
  }
});
