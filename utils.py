import yaml

def load_config(config_file, logConfig):
    with open(config_file, 'r') as f:
        #if logConfig: log.debug("Loading config file " + config_file + "...")
        config = yaml.safe_load(f)
        #if logConfig: log.debug("Loaded config:\n" +yaml.dump(config, default_flow_style=False, sort_keys=False))
    return config

def short(value, length):
    if len(str(value)) > length:
        return str(value)[:length] + "..."
    else:
        return str(value)