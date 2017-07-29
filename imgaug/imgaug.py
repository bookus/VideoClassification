from __future__ import print_function, division, absolute_import
from abc import ABCMeta, abstractmethod
import random
import numpy as np
import copy
import numbers
import cv2
import math
from scipy import misc
import multiprocessing
import threading
import sys
import six
import six.moves as sm

if sys.version_info[0] == 2:
    import cPickle as pickle
    from Queue import Empty as QueueEmpty
elif sys.version_info[0] == 3:
    import pickle
    from queue import Empty as QueueEmpty
    xrange = range

ALL = "ALL"

# We instantiate a current/global random state here once.
# One can also call np.random, but that is (in contrast to np.random.RandomState)
# a module and hence cannot be copied via deepcopy. That's why we use RandomState
# here (and in all augmenters) instead of np.random.
CURRENT_RANDOM_STATE = np.random.RandomState(42)

def seed(seedval):
    CURRENT_RANDOM_STATE.seed(seedval)

def is_np_array(val):
    return isinstance(val, (np.ndarray, np.generic))


def is_single_integer(val):
    return isinstance(val, numbers.Integral)


def is_single_float(val):
    return isinstance(val, numbers.Real) and not is_single_integer(val)


def is_single_number(val):
    return is_single_integer(val) or is_single_float(val)


def is_iterable(val):
    return isinstance(val, (tuple, list))


def is_string(val):
    return isinstance(val, str) or isinstance(val, unicode)


def is_integer_array(val):
    return issubclass(val.dtype.type, np.integer)


def current_random_state():
    return CURRENT_RANDOM_STATE


def new_random_state(seed=None, fully_random=False):
    if seed is None:
        if not fully_random:
            # sample manually a seed instead of just RandomState(),
            # because the latter one
            # is way slower.
            seed = CURRENT_RANDOM_STATE.randint(0, 10**6, 1)[0]
    return np.random.RandomState(seed)


def dummy_random_state():
    return np.random.RandomState(1)


def copy_random_state(random_state, force_copy=False):
    if random_state == np.random and not force_copy:
        return random_state
    else:
        rs_copy = dummy_random_state()
        orig_state = random_state.get_state()
        rs_copy.set_state(orig_state)
        return rs_copy

# TODO
# def from_json(json_str):
#    pass

def angle_between_vectors(v1, v2):
    """ Returns the angle in radians between vectors 'v1' and 'v2':
            >>> angle_between((1, 0, 0), (0, 1, 0))
            1.5707963267948966
            >>> angle_between((1, 0, 0), (1, 0, 0))
            0.0
            >>> angle_between((1, 0, 0), (-1, 0, 0))
            3.141592653589793
        From http://stackoverflow.com/questions/2827393/angles-between-two-n-dimensional-vectors-in-python
    """
    v1_u = v1 / np.linalg.norm(v1)
    v2_u = v2 / np.linalg.norm(v2)
    return np.arccos(np.clip(np.dot(v1_u, v2_u), -1.0, 1.0))

def draw_text(img, y, x, text, color=[0, 255, 0], size=25):
    # keeping PIL here so that it is not a depdency of the library right now
    from PIL import Image, ImageDraw, ImageFont

    assert img.dtype in [np.uint8, np.float32]

    input_dtype = img.dtype
    if img.dtype == np.float32:
        img = img.astype(np.uint8)
        is_float32 = False

    for i in range(len(color)):
        val = color[i]
        if isinstance(val, float):
            val = int(val * 255)
            val = np.clip(val, 0, 255)
            color[i] = val

    shape = img.shape
    img = Image.fromarray(img)
    font = ImageFont.truetype("DejaVuSans.ttf", size)
    context = ImageDraw.Draw(img)
    context.text((x, y), text, fill=tuple(color), font=font)
    img_np = np.asarray(img)
    img_np.setflags(write=True)  # PIL/asarray returns read only array

    if img_np.dtype != input_dtype:
        img_np = img_np.astype(input_dtype)

    return img_np

def imresize_many_images(images, sizes=None, interpolation=None):
    s = images.shape
    assert len(s) == 4, s
    nb_images = s[0]
    im_height, im_width = s[1], s[2]
    nb_channels = s[3]
    height, width = sizes[0], sizes[1]

    if height == im_height and width == im_width:
        return np.copy(images)

    ip = interpolation
    assert ip is None or ip in ["nearest", "linear", "area", "cubic", cv2.INTER_NEAREST, cv2.INTER_LINEAR, cv2.INTER_AREA, cv2.INTER_CUBIC]
    if ip is None:
        if height > im_height or width > im_width:
            ip = cv2.INTER_AREA
        else:
            ip = cv2.INTER_LINEAR
    elif ip in ["nearest", cv2.INTER_NEAREST]:
        ip = cv2.INTER_NEAREST
    elif ip in ["linear", cv2.INTER_LINEAR]:
        ip = cv2.INTER_LINEAR
    elif ip in ["area", cv2.INTER_AREA]:
        ip = cv2.INTER_AREA
    elif ip in ["cubic", cv2.INTER_CUBIC]:
        ip = cv2.INTER_CUBIC
    else:
        raise Exception("Invalid interpolation order")

    result = np.zeros((nb_images, height, width, nb_channels), dtype=np.uint8)
    for img_idx in sm.xrange(nb_images):
        result_img = cv2.resize(images[img_idx], (width, height), interpolation=ip)
        if len(result_img.shape) == 2:
            result_img = result_img[:, :, np.newaxis]
        result[img_idx] = result_img
    return result


def imresize_single_image(image, sizes, interpolation=None):
    grayscale = False
    if image.shape == 2:
        grayscale = True
        image = image[:, :, np.newaxis]
    assert len(image.shape) == 3, image.shape
    rs = imresize_many_images(image[np.newaxis, :, :, :], sizes, interpolation=interpolation)
    if grayscale:
        return np.squeeze(rs[0, :, :, 0])
    else:
        return rs[0, ...]


def draw_grid(images, rows=None, cols=None):
    if is_np_array(images):
        assert images.ndim == 4
    else:
        assert is_iterable(images) and is_np_array(images[0]) and images[0].ndim == 3

    nb_images = len(images)
    cell_height = max([image.shape[0] for image in images])
    cell_width = max([image.shape[1] for image in images])
    channels = set([image.shape[2] for image in images])
    assert len(channels) == 1
    nb_channels = list(channels)[0]
    if rows is None and cols is None:
        rows = cols = int(math.ceil(math.sqrt(nb_images)))
    elif rows is not None:
        cols = int(math.ceil(nb_images / rows))
    elif cols is not None:
        rows = int(math.ceil(nb_images / cols))
    assert rows * cols >= nb_images

    width = cell_width * cols
    height = cell_height * rows
    grid = np.zeros((height, width, nb_channels), dtype=np.uint8)
    cell_idx = 0
    for row_idx in sm.xrange(rows):
        for col_idx in sm.xrange(cols):
            if cell_idx < nb_images:
                image = images[cell_idx]
                cell_y1 = cell_height * row_idx
                cell_y2 = cell_y1 + image.shape[0]
                cell_x1 = cell_width * col_idx
                cell_x2 = cell_x1 + image.shape[1]
                grid[cell_y1:cell_y2, cell_x1:cell_x2, :] = image
            cell_idx += 1

    return grid


def show_grid(images, rows=None, cols=None):
    grid = draw_grid(images, rows=rows, cols=cols)
    misc.imshow(grid)


class HooksImages(object):
    """
    # TODO
    """
    def __init__(self, activator=None, propagator=None, preprocessor=None, postprocessor=None):
        self.activator = activator
        self.propagator = propagator
        self.preprocessor = preprocessor
        self.postprocessor = postprocessor

    def is_activated(self, images, augmenter, parents, default):
        if self.activator is None:
            return default
        else:
            return self.activator(images, augmenter, parents, default)

    # TODO is a propagating hook necessary? seems to be covered by activated
    # hook already
    def is_propagating(self, images, augmenter, parents, default):
        if self.propagator is None:
            return default
        else:
            return self.propagator(images, augmenter, parents, default)

    def preprocess(self, images, augmenter, parents):
        if self.preprocessor is None:
            return images
        else:
            return self.preprocessor(images, augmenter, parents)

    def postprocess(self, images, augmenter, parents):
        if self.postprocessor is None:
            return images
        else:
            return self.postprocessor(images, augmenter, parents)


class HooksKeypoints(HooksImages):
    pass


class Keypoint(object):
    """
    # TODO
    """

    def __init__(self, x, y):
        # these checks are currently removed because they are very slow for some
        # reason
        #assert is_single_integer(x), type(x)
        #assert is_single_integer(y), type(y)
        self.x = x
        self.y = y

    def project(self, from_shape, to_shape):
        if from_shape[0:2] == to_shape[0:2]:
            return Keypoint(x=self.x, y=self.y)
        else:
            from_height, from_width = from_shape[0:2]
            to_height, to_width = to_shape[0:2]
            x = int(round((self.x / from_width) * to_width))
            y = int(round((self.y / from_height) * to_height))
            return Keypoint(x=x, y=y)

    def shift(self, x, y):
        return Keypoint(self.x + x, self.y + y)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "Keypoint(x=%d, y=%d)" % (self.x, self.y)


class KeypointsOnImage(object):
    def __init__(self, keypoints, shape):
        self.keypoints = keypoints
        if is_np_array(shape):
            self.shape = shape.shape
        else:
            assert isinstance(shape, (tuple, list))
            self.shape = tuple(shape)

    @property
    def height(self):
        return self.shape[0]

    @property
    def width(self):
        return self.shape[1]

    def on(self, image):
        if is_np_array(image):
            shape = image.shape
        else:
            shape = image

        if shape[0:2] == self.shape[0:2]:
            return self.deepcopy()
        else:
            keypoints = [kp.project(self.shape, shape) for kp in self.keypoints]
            return KeypointsOnImage(keypoints, shape)

    def draw_on_image(self, image, color=[0, 255, 0], size=3, copy=True, raise_if_out_of_image=False):
        if copy:
            image = np.copy(image)

        height, width = image.shape[0:2]

        for keypoint in self.keypoints:
            y, x = keypoint.y, keypoint.x
            if 0 <= y < height and 0 <= x < width:
                x1 = max(x - size//2, 0)
                x2 = min(x + 1 + size//2, width - 1)
                y1 = max(y - size//2, 0)
                y2 = min(y + 1 + size//2, height - 1)
                image[y1:y2, x1:x2] = color
            else:
                if raise_if_out_of_image:
                    raise Exception("Cannot draw keypoint x=%d, y=%d on image with shape %s." % (y, x, image.shape))

        return image

    def shift(self, x, y):
        keypoints = [keypoint.shift(x=x, y=y) for keypoint in self.keypoints]
        return KeypointsOnImage(keypoints, self.shape)

    def get_coords_array(self):
        result = np.zeros((len(self.keypoints), 2), np.int32)
        for i, keypoint in enumerate(self.keypoints):
            result[i, 0] = keypoint.x
            result[i, 1] = keypoint.y
        return result

    @staticmethod
    def from_coords_array(coords, shape):
        assert is_integer_array(coords), coords.dtype
        keypoints = [Keypoint(x=coords[i, 0], y=coords[i, 1]) for i in sm.xrange(coords.shape[0])]
        return KeypointsOnImage(keypoints, shape)

    def to_keypoint_image(self):
        assert len(self.keypoints) > 0
        height, width = self.shape[0:2]
        image = np.zeros((height, width, len(self.keypoints)), dtype=np.uint8)
        for i, keypoint in enumerate(self.keypoints):
            y = keypoint.y
            x = keypoint.x
            if 0 <= y < height and 0 <= x < width:
                image[y, x, i] = 255
        return image

    @staticmethod
    def from_keypoint_image(image, if_not_found_coords={"x": -1, "y": -1}, threshold=1):
        assert len(image.shape) == 3
        height, width, nb_keypoints = image.shape

        drop_if_not_found = False
        if if_not_found_coords is None:
            drop_if_not_found = True
            if_not_found_x = -1
            if_not_found_y = -1
        elif isinstance(if_not_found_coords, (tuple, list)):
            assert len(if_not_found_coords) == 2
            if_not_found_x = if_not_found_coords[0]
            if_not_found_y = if_not_found_coords[1]
        elif isinstance(if_not_found_coords, dict):
            if_not_found_x = if_not_found_coords["x"]
            if_not_found_y = if_not_found_coords["y"]
        else:
            raise Exception("Expected if_not_found_coords to be None or tuple or list or dict, got %s." % (type(if_not_found_coords),))

        keypoints = []
        for i in sm.xrange(nb_keypoints):
            maxidx_flat = np.argmax(image[..., i])
            maxidx_ndim = np.unravel_index(maxidx_flat, (height, width))
            found = (image[maxidx_ndim[0], maxidx_ndim[1], i] >= threshold)
            if found:
                keypoints.append(Keypoint(x=maxidx_ndim[1], y=maxidx_ndim[0]))
            else:
                if drop_if_not_found:
                    pass # dont add the keypoint to the result list, i.e. drop it
                else:
                    keypoints.append(Keypoint(x=if_not_found_x, y=if_not_found_y))

        return KeypointsOnImage(keypoints, shape=(height, width))

    def copy(self):
        return copy.copy(self)

    def deepcopy(self):
        # for some reason deepcopy is way slower here than manual copy
        #return copy.deepcopy(self)
        kps = [Keypoint(x=kp.x, y=kp.y) for kp in self.keypoints]
        return KeypointsOnImage(kps, tuple(self.shape))

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        #print(type(self.keypoints), type(self.shape))
        return "KeypointOnImage(%s, shape=%s)" % (str(self.keypoints), self.shape)


############################
# Background augmentation
############################

class Batch(object):
    """Class encapsulating a batch before and after augmentation."""
    def __init__(self, images=None, keypoints=None, data=None):
        self.images = images
        self.images_aug = None
        # keypoints here are the corners of the bounding box
        self.keypoints = keypoints
        self.keypoints_aug = None
        self.data = data

class BatchLoader(object):
    """Class to load batches in the background."""

    def __init__(self, load_batch_func, queue_size=50, nb_workers=1, threaded=True):
        assert queue_size > 0
        assert nb_workers >= 1
        self.queue = multiprocessing.Queue(queue_size)
        self.join_signal = multiprocessing.Event()
        self.finished_signals = []
        self.workers = []
        self.threaded = threaded
        seeds = current_random_state().randint(0, 10**6, size=(nb_workers,))
        for i in range(nb_workers):
            finished_signal = multiprocessing.Event()
            self.finished_signals.append(finished_signal)
            if threaded:
                worker = threading.Thread(target=self._load_batches, args=(load_batch_func, self.queue, finished_signal, self.join_signal, None))
            else:
                worker = multiprocessing.Process(target=self._load_batches, args=(load_batch_func, self.queue, finished_signal, self.join_signal, seeds[i]))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def all_finished(self):
        return all([event.is_set() for event in self.finished_signal])

    def _load_batches(self, load_batch_func, queue, finished_signal, join_signal, seedval):
        if seedval is not None:
            random.seed(seedval)
            np.random.seed(seedval)
            seed(seedval)

        for batch in load_batch_func():
            assert isinstance(batch, Batch), "Expected batch returned by lambda function to be of class imgaug.Batch, got %s." % (type(batch),)
            queue.put(pickle.dumps(batch, protocol=-1))
            if join_signal.is_set():
                break

        finished_signal.set()

    def terminate(self):
        self.join_signal.set()
        if self.threaded:
            for worker in self.workers:
                worker.join()
        else:
            for worker, finished_signal in zip(self.workers, self.finished_signals):
                worker.terminate()
                finished_signal.set()

class BackgroundAugmenter(object):
    """Class to augment batches in the background (while training on
    the GPU)."""
    def __init__(self, batch_loader, augseq, queue_size=50, nb_workers="auto"):
        assert queue_size > 0
        self.augseq = augseq
        self.source_finished_signals = batch_loader.finished_signals
        self.queue_source = batch_loader.queue
        self.queue_result = multiprocessing.Queue(queue_size)

        if nb_workers == "auto":
            try:
                nb_workers = multiprocessing.cpu_count()
            except (ImportError, NotImplementedError):
                nb_workers = 1
            # try to reserve at least one core for the main process
            nb_workers = max(1, nb_workers - 1)
        else:
            assert nb_workers >= 1
        #print("Starting %d background processes" % (nb_workers,))

        self.nb_workers = nb_workers
        self.workers = []
        self.nb_workers_finished = 0

        self.augment_images = True
        self.augment_keypoints = True

        seeds = current_random_state().randint(0, 10**6, size=(nb_workers,))
        for i in range(nb_workers):
            worker = multiprocessing.Process(target=self._augment_images_worker, args=(augseq, self.queue_source, self.queue_result, self.source_finished_signals, seeds[i]))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def get_batch(self):
        """Returns a batch from the queue of augmented batches."""
        batch_str = self.queue_result.get()
        batch = pickle.loads(batch_str)
        if batch is not None:
            return batch
        else:
            self.nb_workers_finished += 1
            if self.nb_workers_finished == self.nb_workers:
                return None
            else:
                return self.get_batch()

    def _augment_images_worker(self, augseq, queue_source, queue_result, source_finished_signals, seedval):
        """Worker function that endlessly queries the source queue (input
        batches), augments batches in it and sends the result to the output
        queue."""
        np.random.seed(seedval)
        random.seed(seedval)
        augseq.reseed(seedval)
        seed(seedval)

        while True:
            # wait for a new batch in the source queue and load it
            try:
                batch_str = queue_source.get(timeout=0.1)
                batch = pickle.loads(batch_str)
                # augment the batch
                batch_augment_images = batch.images is not None and self.augment_images
                batch_augment_keypoints = batch.keypoints is not None and self.augment_keypoints

                if batch_augment_images and batch_augment_keypoints:
                    augseq_det = augseq.to_deterministic()
                    batch.images_aug = augseq_det.augment_images(batch.images)
                    batch.keypoints_aug = augseq_det.augment_keypoints(batch.keypoints)
                elif batch_augment_images is not None:
                    batch.images_aug = augseq.augment_images(batch.images)
                elif batch_augment_keypoints is not None:
                    batch.keypoints_aug = augseq.augment_keypoints(batch.keypoints)

                # send augmented batch to output queue
                batch_str = pickle.dumps(batch, protocol=-1)
                queue_result.put(batch_str)
            except QueueEmpty as e:
                if all([signal.is_set() for signal in source_finished_signals]):
                    queue_result.put(pickle.dumps(None, protocol=-1))
                    return

    def terminate(self):
        for worker in self.workers:
            worker.terminate()