# transforms.py

import numpy as np
import torch

class BinaryToFloatTensor(object):
    def __call__(self, pic):
        """
        Convert a byte buffer representation of an image to a float tensor.

        Args:
            pic (bytes): A byte buffer representing an image.

        Returns:
            torch.Tensor: A float tensor representing the image.

        """
        np_array = np.frombuffer(pic, dtype=np.uint8).reshape(28, 28).copy()
        return torch.from_numpy(np_array).float()
