const docsURL = 'http://bitcore.io/';

export const ERROR_TYPES = {
  InvalidB58Char: {
    message: 'Invalid Base58 character: {0} in {1}'
  },

  InvalidB58Checksum: {
    message: 'Invalid Base58 checksum for {0}'
  },

  InvalidNetwork: {
    message: 'Invalid version for network: got {0}'
  },

  InvalidState: {
    message: 'Invalid state: {0}'
  },

  NotImplemented: {
    message: 'Function {0} was not implemented yet'
  },

  InvalidNetworkArgument: {
    message: 'Invalid network: must be "livenet" or "testnet", got {0}'
  },

  InvalidArgument: {
    message(args?: Array<string>) {
      return (
        'Invalid Argument {0} ' +
        (args[0] ? ': ' + args[0] : '') +
        (args[1] ? ' Documentation: ' + docsURL + args[1] : '')
      );
    }
  },

  AbstractMethodInvoked: {
    message: 'Abstract Method Invocation: {0}'
  },

  InvalidArgumentType: {
    message(args: Array<string>) {
      return (
        'Invalid Argument for ' +
        args[2] +
        ', expected ' +
        args[1] +
        ' but got ' +
        typeof args[0]
      );
    }
  },

  Unit: {
    message: 'Internal Error on Unit {0}',
    errors: {
      UnknownCode: {
        message: 'Unrecognized unit code: {0}'
      },

      InvalidRate: {
        message: 'Invalid exchange rate: {0}'
      }
    }
  },

  MerkleBlock: {
    message: 'Internal Error on MerkleBlock {0}',
    errors: {
      InvalidMerkleTree: {
        message: 'This MerkleBlock contain an invalid Merkle Tree'
      }
    }
  },

  Transaction: {
    message: 'Internal Error on Transaction {0}',
    errors: {
      Input: {
        message: 'Internal Error on Input {0}',
        errors: {
          MissingScript: {
            message: 'Need a script to create an input'
          },

          UnsupportedScript: {
            message: 'Unsupported input script type: {0}'
          },

          MissingPreviousOutput: {
            message: 'No previous output information.'
          }
        }
      },

      NeedMoreInfo: {
        message: '{0}'
      },

      InvalidSorting: {
        message:
          'The sorting function provided did not return the change output as one of the array elements'
      },

      InvalidOutputAmountSum: {
        message: '{0}'
      },

      MissingSignatures: {
        message: 'Some inputs have not been fully signed'
      },

      InvalidIndex: {
        message: 'Invalid index: {0} is not between 0, {1}'
      },

      UnableToVerifySignature: {
        message: 'Unable to verify signature: {0}'
      },

      DustOutputs: {
        message: 'Dust amount detected in one output'
      },

      InvalidSatoshis: {
        message: 'Output satoshis are invalid'
      },

      FeeError: {
        message: 'Internal Error on Fee {0}',
        errors: {
          TooSmall: {
            message: 'Fee is too small: {0}'
          },

          TooLarge: {
            message: 'Fee is too large: {0}'
          },

          Different: {
            message: 'Unspent value is different from specified fee: {0}'
          }
        }
      },

      ChangeAddressMissing: {
        message: 'Change address is missing'
      },

      BlockHeightTooHigh: {
        message: 'Block Height can be at most 2^32 -1'
      },

      NLockTimeOutOfRange: {
        message: 'Block Height can only be between 0 and 499 999 999'
      },

      LockTimeTooEarly: {
        message: "Lock Time can't be earlier than UNIX date 500 000 000"
      }
    }
  },

  Script: {
    message: 'Internal Error on Script {0}',
    errors: {
      UnrecognizedAddress: {
        message: 'Expected argument {0} to be an address'
      },

      CantDeriveAddress: {
        message:
          "Can't derive address associated with script {0}, needs to be p2pkh in, p2pkh out, p2sh in, or p2sh out."
      },

      InvalidBuffer: {
        message:
          "Invalid script buffer: can't parse valid script from given buffer {0}"
      }
    }
  },

  HDPrivateKey: {
    message: 'Internal Error on HDPrivateKey {0}',
    errors: {
      InvalidDerivationArgument: {
        message:
          'Invalid derivation argument {0}, expected string, or number and boolean'
      },

      InvalidEntropyArgument: {
        message:
          'Invalid entropy: must be an hexa string or binary buffer, got {0}',
        errors: {
          TooMuchEntropy: {
            message:
              'Invalid entropy: more than 512 bits is non standard, got "{0}"'
          },

          NotEnoughEntropy: {
            message: 'Invalid entropy: at least 128 bits needed, got "{0}"'
          }
        }
      },

      InvalidLength: {
        message: 'Invalid length for xprivkey string in {0}'
      },

      InvalidPath: {
        message: 'Invalid derivation path: {0}'
      },

      UnrecognizedArgument: {
        message:
          'Invalid argument: creating a HDPrivateKey requires a string, buffer, json or object, got "{0}"'
      }
    }
  },

  HDPublicKey: {
    message: 'Internal Error on HDPublicKey {0}',
    errors: {
      ArgumentIsPrivateExtended: {
        message: 'Argument is an extended private key: {0}'
      },

      InvalidDerivationArgument: {
        message: 'Invalid derivation argument: got {0}'
      },

      InvalidLength: {
        message: 'Invalid length for xpubkey: got "{0}"'
      },

      InvalidPath: {
        message:
          'Invalid derivation path, it should look like: "m/1/100", got "{0}"'
      },

      InvalidIndexCantDeriveHardened: {
        message:
          'Invalid argument: creating a hardened path requires an HDPrivateKey'
      },

      MustSupplyArgument: {
        message: 'Must supply an argument to create a HDPublicKey'
      },

      UnrecognizedArgument: {
        message:
          'Invalid argument for creation, must be string, json, buffer, or object'
      }
    }
  }
};
