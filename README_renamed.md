# Challenge General Information

You can read the details of the challenge at [challenge.md](challenge.md)

## Key Items

- __Due Date:__ December 13, 2023
- __Work Rules:__ You cannot work with others.  You can ask any question you want in our general channel. Teacher and TA are the only one that can answer questions. If you leverage code from an internet connection, then it should be referenced.
- __Product:__ A streamlit app that runs within Docker and builds from your repo. Additionally, a fully documented `readme.md`.
- __Github Process:__ Each student will fork the challenge repository and create their app. They will submit a link to the app in Canvas.
- __Canvas Process:__ Each student will upload a `.pdf` or `.html` file with your results as described in [challenge.md](challenge.md)


## Notes & References

- [Fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)
- [Creating a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)

### Docker Information

Within this repository, you can simply run `docker compose up` to leverage the `docker-compose.yaml` with your local folder synced with the container folder where the streamlit app is running. 

Additionally, you can use `docker build -t streamlit .` to use the `Dockerfile` to build the image and then use `docker run -p 8501:8501 -v "$(pwd):/app:rw" streamlit` to start the container with the appropriate port and volume settings.

We currently use `python:3.11.6-slim` in our [Dockerfile](Dockerfile).  You can change to `FROM quay.io/jupyter/minimal-notebook` to use the [minimal Jupyter notebook](https://quay.io/organization/jupyter)
