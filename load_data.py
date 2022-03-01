import argparse
import csv
import datetime as dt
import logging
import multiprocessing
import os
from typing import Dict, Tuple, List

import dateutil.parser
import intervaltree
import pandas as pd
import simplejson as json

import dataset.util as util
from dataset.data.extract import load_hek_data, load_goes_flux, goes_files, load_all_goes_profiles
from dataset.data.load import sample_path, sample_exists, RequestSender, ImageLoader, OutputProcessor
from dataset.data.transform import extract_events, map_flares, active_region_time_ranges, sample_ranges, verify_sampling

DEFAULT_ARGS = {
    "start": dt.datetime(2012, 1, 1),
    "end": dt.datetime(2018, 1, 1),
    "input_hours": 12,
    "output_hours": 24,
    "time_steps": [0, 7*60, 10*60+30, 11*60+50],
    "seed": 726527
}

logger = logging.getLogger(__name__)

# TODO: Handle the merging and splitting of ARs. Sometimes ARs "suddenly?" get a new number
# TODO: Check if active regions overlap each other to avoid duplicates


def main():
    args = parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    util.configure_logging(log_level)

    path_helper = util.PathHelper(args.directory, args.fitsdir)

    # Data loading
    load_raw(path_helper.raw_directory, args.start, args.end)


    date_suffix = _date_suffix(args.start, args.end)
    transform_raw(
        dt.timedelta(hours=args.input_hours),
        dt.timedelta(hours=args.output_hours),
        path_helper.raw_directory,
        path_helper.intermediate_directory,
        date_suffix,
        args.seed
    )

    create_output(
        path_helper,
        args.email,
        args.time_steps,
        date_suffix
    )


def load_raw(output_directory: str, start: dt.datetime, end: dt.datetime):
    logger.info("Loading raw data")

    if not os.path.isdir(output_directory):
        logger.info("Creating output directory %s", output_directory)
        os.makedirs(output_directory, exist_ok=False)

    date_suffix = _date_suffix(start, end)

    # GOES flux
    goes_raw_path = os.path.join(output_directory, "goes")
    logger.info("Downloading GOES flux to %s ...", goes_raw_path)

    os.makedirs(goes_raw_path, exist_ok=True)

    for target_file_name, current_date in goes_files(start, end):
        target_file_path = os.path.join(goes_raw_path, target_file_name)

        if not os.path.exists(target_file_path):
            raw_flux_data = load_goes_flux(current_date)
            if raw_flux_data is not None:
                with open(target_file_path, "w") as f:
                    f.write(raw_flux_data)

    logger.info("GOES flux files are ready")

    # HEK events
    events_raw_path = os.path.join(output_directory, f"events_{date_suffix}.json")

    if os.path.isfile(events_raw_path):
        logger.info("Using existing event list at %s", events_raw_path)
    else:
        logger.info("Event list not found, will be downloaded to %s", events_raw_path)

        with open(events_raw_path, "w") as f:
            hek_events = load_hek_data(start, end)
            import numpy as np
            np.set_printoptions(precision=4)
            json.dump(hek_events, f, iterable_as_array=True)

        logger.info("Loaded event list")


def _date_suffix(start: dt.datetime, end: dt.datetime) -> str:
    # Use a date format which does not produce characters illegal in mainstream operating systems
    return f"{start:%Y-%m-%dT%H%M%S}_{end:%Y-%m-%dT%H%M%S}"


def transform_raw(
        input_duration: dt.timedelta,
        output_duration: dt.timedelta,
        input_directory: str,
        output_directory: str,
        date_suffix: str,
        seed: int
):
    logger.info("Transforming raw data")

    if not os.path.isdir(output_directory):
        logger.info("Creating output directory %s", output_directory)
        os.makedirs(output_directory, exist_ok=False)

    logger.debug("Loading saved events")
    events_raw_path = os.path.join(input_directory, f"events_{date_suffix}.json")
    with open(events_raw_path, "r") as f:
        raw_events = json.load(f)

    # Loading goes curves on demand
    goes = None

    logger.info('Extracting events from raw...')
    swpc_flares, noaa_active_regions = extract_events(raw_events)
    logger.debug(
        "Extracted %d SWPC dataset and %d (grouped) NOAA active regions", len(swpc_flares), len(noaa_active_regions)
    )

    ranges_path = os.path.join(output_directory, f"ranges_{date_suffix}.csv")

    if os.path.isfile(ranges_path):
        logger.info("Using existing ranges at %s", ranges_path)
    else:
        logger.info("Ranges not found, will be computed to %s", ranges_path)

        if goes is None:
            # load GOES curves
            logger.info('Loading GOES curves...')
            goes = load_all_goes_profiles(os.path.join(input_directory, "goes"))

        logger.info('Mapping flares...')
        mapped_flares, unmapped_flares = map_flares(swpc_flares, noaa_active_regions, raw_events)
        logger.debug(
            "Created flare mapping, resulting in %d mapped and %d unmapped dataset",
            len(mapped_flares), len(unmapped_flares)
        )

        logger.info('Computing ranges. This might take an hour or two...')
        ranges = active_region_time_ranges(
            input_duration, output_duration, noaa_active_regions, mapped_flares, unmapped_flares, goes, os.path.join(input_directory, "goes")
        )
        logger.info("Computed ranges")

        _save_ranges(ranges_path, ranges)
        logger.info("Saved ranges")

    samples_test_path = os.path.join(output_directory, f"samples_test_{date_suffix}.csv")
    samples_training_path = os.path.join(output_directory, f"samples_training_{date_suffix}.csv")

    if os.path.isfile(samples_test_path) and os.path.isfile(samples_training_path):
        logger.info("Using existing test/training sets at %s and %s", samples_test_path, samples_training_path)
    else:
        logger.info(
            "Test/training sets not found, will be sampled to %s and %s", samples_test_path, samples_training_path
        )

        all_ranges = pd.read_csv(
            ranges_path,
            delimiter=";",
            index_col=0,
            parse_dates=["start", "end", "peak"]
        )

        test_samples, training_samples = sample_ranges(
            all_ranges,
            input_duration,
            output_duration,
            seed
        )
        # id for each sample is range id + sample index, can be used to filter samples from same ranges
        test_samples.to_csv(samples_test_path, sep=";", index_label="id")
        training_samples.to_csv(samples_training_path, sep=";", index_label="id")
        logger.info("Sampled test/training sets")

        logger.info("Verifying sampling")
        test_samples = pd.read_csv(
            samples_test_path,
            delimiter=";",
            index_col=0,
            parse_dates=["start", "end", "peak"]
        )
        training_samples = pd.read_csv(
            samples_training_path,
            delimiter=";",
            index_col=0,
            parse_dates=["start", "end", "peak"]
        )

        if goes is None:
            # load GOES curves
            logger.info('Loading GOES curves...')
            goes = load_all_goes_profiles(os.path.join(input_directory, "goes"))

        verify_sampling(test_samples, training_samples, input_duration, output_duration, noaa_active_regions, goes)
        logger.info("Sampling verified successfully")


def create_output(
        path_helper: util.PathHelper,
        email_address: str,
        time_steps: List[int],
        date_suffix: str
):
    logger.info("Creating output")

    # Create directories
    test_directory = os.path.join(path_helper.output_directory, date_suffix, "test")
    training_directory = os.path.join(path_helper.output_directory, date_suffix, "training")
    test_fits_directory = os.path.join(path_helper.fits_directory, date_suffix, "test")
    training_fits_directory = os.path.join(path_helper.fits_directory, date_suffix, "training")

    # Load sample data
    logger.debug("Loading sample data from csv")
    samples_test_path = os.path.join(path_helper.intermediate_directory, f"samples_test_{date_suffix}.csv")
    samples_training_path = os.path.join(path_helper.intermediate_directory, f"samples_training_{date_suffix}.csv")
    test_samples = pd.read_csv(
        samples_test_path,
        delimiter=";",
        index_col=0,
        parse_dates=["start", "end", "peak"]
    )
    training_samples = pd.read_csv(
        samples_training_path,
        delimiter=";",
        index_col=0,
        parse_dates=["start", "end", "peak"]
    )

    # Load active regions
    logger.debug("Loading saved events")
    events_raw_path = os.path.join(path_helper.raw_directory, f"events_{date_suffix}.json")
    with open(events_raw_path, "r") as f:
        raw_events = json.load(f)
    _, noaa_active_regions = extract_events(raw_events)


    logger.info("Creating training samples")
    _create_output(training_samples, training_directory, training_fits_directory, email_address, time_steps, noaa_active_regions)

    logger.info("Creating test samples")
    _create_output(test_samples, test_directory, test_fits_directory, email_address, time_steps, noaa_active_regions)


def _create_output(
        samples: pd.DataFrame,
        output_directory: str,
        fits_directory: str,
        email_address: str,
        time_steps: List[int],
        noaa_regions: Dict[int, Tuple[dt.datetime, dt.datetime, List[dict]]]
):
    if not os.path.isdir(output_directory):
        logger.debug("Creating output directory %s", output_directory)
        os.makedirs(output_directory, exist_ok=False)
        os.makedirs(fits_directory, exist_ok=True)

    # Create meta data file
    meta_file = os.path.join(output_directory, "meta_data.csv")
    # put only id, start, end and peak_flux into csv. Discard noaa_num, type and peak
    samples[['start','end','peak_flux']].to_csv(meta_file, sep=",", index_label="id")
    logger.info("Wrote meta data file")

    _create_image_output(samples, output_directory, fits_directory, email_address, time_steps, noaa_regions)
    logger.info("Wrote samples")


def _create_image_output(
        samples: pd.DataFrame,
        output_directory: str,
        fits_directory: str,
        email_address: str,
        time_steps: List[int],
        noaa_regions: Dict[int, Tuple[dt.datetime, dt.datetime, List[dict]]]
):
    # Create a list of samples which are to be created
    target_samples = [
        (sample_id, sample_values)
        for sample_id, sample_values in samples.iterrows()
        if not sample_exists(sample_path(sample_id, output_directory))
    ]
    logger.info("%d samples will be created", len(target_samples))

    p = 64

    # Create pools for different download steps
    with multiprocessing.Pool(processes=8) as request_pool, \
            multiprocessing.Pool(processes=p) as download_pool, \
            multiprocessing.Pool(processes=p) as process_pool, \
            multiprocessing.Manager() as manager:
        # Queues for synchronisation
        download_queue = manager.Queue(maxsize=p*4)
        processing_queue = manager.Queue(maxsize=p*4)
        cache_queue = manager.Queue()

        # Create workers
        request_sender = RequestSender(download_queue, cache_queue, output_directory, email_address, time_steps, os.path.join(output_directory, 'requestsCache.json'))
        image_loader = ImageLoader(download_queue, processing_queue, output_directory, fits_directory)
        output_processor = OutputProcessor(processing_queue, output_directory, fits_directory, samples, noaa_regions, time_steps)

        # Start workers
        logger.debug("Starting output processor workers")
        output_processor_results = [process_pool.apply_async(output_processor) for _ in range(p)]
        logger.debug("Starting image loader workers")
        image_loader_results = [download_pool.apply_async(image_loader) for _ in range(p)]

        # Map inputs to finally start full process
        logger.debug("Starting requests")
        request_pool.map(request_sender, target_samples) #target_samples[:10]
        request_sender.terminate()
        logger.info("Finished requests")

        # Wait for image loader workers to finish
        logger.info("Waiting for image loader workers to finish")
        for _ in range(p):
            download_queue.put(None)
        for current_worker_result in image_loader_results:
            current_worker_result.get()

        # Wait for output processor workers to finish
        logger.info("Waiting for output processor workers to finish")
        for _ in range(p):
            processing_queue.put(None)
        for current_worker_result in output_processor_results:
            current_worker_result.get()

        logger.info("All workers finished")

def _parse_goes_flux(file_path: str) -> pd.DataFrame:
    with open(file_path, "r") as f:
        # Skip lines until data: label is read
        for line in f:
            if line.startswith("data:"):
                break

        return pd.read_csv(f, sep=",", parse_dates=["time_tag"], index_col="time_tag")


def _save_ranges(output_path: str, ranges: Dict[int, intervaltree.IntervalTree]):
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter=";")

        # Header
        writer.writerow(("id", "noaa_num", "start", "end", "type", "peak", "peak_flux"))

        for noaa_num, region_ranges in ranges.items():
            for interval in region_ranges:
                current_id = _range_id(noaa_num, interval)
                current_class, current_peak, current_peak_flux = interval.data

                writer.writerow((
                    current_id,
                    noaa_num,
                    interval.begin.strftime(util.HEK_DATE_FORMAT),
                    interval.end.strftime(util.HEK_DATE_FORMAT),
                    current_class,
                    current_peak.strftime(util.HEK_DATE_FORMAT) if current_peak is not None else None,
                    current_peak_flux
                ))


def _range_id(noaa_num: int, interval: intervaltree.Interval):
    return f"{noaa_num}_{interval.begin:%Y_%m_%d_%H_%M_%S}"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "directory", help="Output directory"
    )
    parser.add_argument(
        "email", help="Registered JSOC email address for image download"
    )
    parser.add_argument(
        "--fitsdir", help="Output directory for FITS files. Optionally different than 'directory'."
    )
    parser.add_argument(
        "--start", default=DEFAULT_ARGS["start"], type=dateutil.parser.parse, help="First date and time (inclusive)"
    )
    parser.add_argument(
        "--end", default=DEFAULT_ARGS["end"], type=dateutil.parser.parse, help="Last date and time (exclusive)"
    )
    parser.add_argument(
        "--input-hours", default=DEFAULT_ARGS["input_hours"], type=int, help="Number of hours for input"
    )
    parser.add_argument(
        "--output-hours", default=DEFAULT_ARGS["output_hours"], type=int, help="Number of hours for output"
    )
    parser.add_argument(
        "--time_steps", default=DEFAULT_ARGS["time_steps"], type=int, help="Input image time stamps after input start (i.e. starting 12h before prediction window)"
    )
    parser.add_argument(
        "--seed", default=DEFAULT_ARGS["seed"], type=int, help="Seed which is used for test/training sampling"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enabled debug logging"
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
