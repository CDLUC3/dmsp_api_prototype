exports.handler = async (event) => {
  // Your function logic goes here
  const response = {
      statusCode: 200,
      body: JSON.stringify('Hello, SAM!'),
  };
  return response;
};