"""Run the FraudStream dashboard inside a container or on a developer machine."""
from app import create_app


if __name__ == "__main__":
    # Cassandra reads can take a short time after a pod restart.  Keep probe
    # requests responsive instead of serialising them behind dashboard reads.
    create_app().run(host="0.0.0.0", port=8080, threaded=True)
