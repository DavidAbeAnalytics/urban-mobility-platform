Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

## CI/CD
This project uses GitHub Actions to run dbt tests automatically on every pull request.

## CI/CD Pipeline
This project uses GitHub Actions to automatically run dbt tests on every pull request.
The workflow installs dbt, configures BigQuery credentials, and runs the full test suite
before any code can merge to main.