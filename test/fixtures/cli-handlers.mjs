// Handlers for the CLI worker tests.
export default {
  echo: ({ job }) => ({ echo: job.payload }),
};
