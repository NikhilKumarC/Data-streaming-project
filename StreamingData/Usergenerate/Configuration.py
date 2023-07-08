import toml

with open("Usergenerate/config.toml") as fp:
    config = toml.load(fp)
secretname = config["AWS-secret"]["secretname"]
region = config["AWS-secret"]["region"]
targetbucket = config["AWS-s3"]["targetbucket"]