try:
    import dlt
    print("dlt version:", dlt.__version__)
except ImportError:
    print("dlt is not installed.")
