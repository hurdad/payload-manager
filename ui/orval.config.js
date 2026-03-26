export default {
  payloadManager: {
    input: '../gateway/openapi/apidocs.swagger.yaml',
    output: {
      target: './src/lib/client/index.ts',
      client: 'fetch',
      mode: 'single'
    }
  }
};
