export default {
  greet: ({ job }) => ({ hello: job.payload }),
  noop: () => undefined,
};
