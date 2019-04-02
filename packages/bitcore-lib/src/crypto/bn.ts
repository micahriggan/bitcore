import $ from '../util/preconditions';
import * as _ from 'lodash';
import BN from 'bn.js';
const HEX_BASE = 16;
const DECIMAL_BASE = 10;
const NEGATIVE_128 = 0x80;
const POSITIVE_127 = 0x7f;

type Endianness = 'le' | 'be';
interface IBufferEncodingOptions {
  size?: number;
  endian?: 'little' | 'big';
}

export class BitcoreBN extends BN {
  public static Zero = new BitcoreBN(0);
  public static One = new BitcoreBN(1);
  public static Minus1 = new BitcoreBN(-1);

  public static fromNumber(n: number) {
    $.checkArgument(_.isNumber(n));
    return new BitcoreBN(n);
  }

  public static fromString(str: string, base: number = 10) {
    $.checkArgument(_.isString(str));
    return new BitcoreBN(str, base);
  }

  public static fromBuffer(buf: Buffer, opts?: IBufferEncodingOptions) {
    if (typeof opts !== 'undefined' && opts.endian === 'little') {
      buf = reversebuf(buf);
    }
    const hex = buf.toString('hex');
    const bn = new BitcoreBN(hex, HEX_BASE);
    return bn;
  }

  /**
   * Instantiate a BigNumber from a "signed magnitude buffer"
   * (a buffer where the most significant bit represents the sign (0 = positive, -1 = negative))
   */
  public static fromSM(buf: Buffer, opts: IBufferEncodingOptions) {
    let ret;
    if (buf.length === 0) {
      return BitcoreBN.fromBuffer(Buffer.from([0]));
    }

    let endian = 'big';
    if (opts) {
      endian = opts.endian;
    }
    if (endian === 'little') {
      buf = reversebuf(buf);
    }

    // -1 & NEGATIVE_128
    if (buf[0] & NEGATIVE_128) {
      buf[0] = buf[0] & POSITIVE_127;
      ret = BitcoreBN.fromBuffer(buf);
      ret.neg().copy(ret);
    } else {
      ret = BitcoreBN.fromBuffer(buf);
    }
    return ret;
  }

  public toNumber() {
    return parseInt(this.toString(DECIMAL_BASE), DECIMAL_BASE);
  }


  public toBuffer(opts?: IBufferEncodingOptions): Buffer;
  public toBuffer(endian?: Endianness, length?: number): Buffer;

  public toBuffer(opts?: Endianness | IBufferEncodingOptions, length?: number) {
    let buf;
    let hex;
    if (opts && typeof opts === 'object') {
      if ((opts as IBufferEncodingOptions).size) {
        hex = this.toString(HEX_BASE, 2);
        const natlen = hex.length / 2;
        buf = Buffer.from(hex, 'hex');

        if (natlen === opts.size) {
          buf = buf;
        } else if (natlen > opts.size) {
          buf = BitcoreBN.trim(buf, natlen);
        } else if (natlen < opts.size) {
          buf = BitcoreBN.pad(buf, natlen, opts.size);
        }
      }
      if (typeof opts !== 'undefined' && opts.endian === 'little') {
        buf = reversebuf(buf);
      }
    } else if (typeof opts === 'string') {
      buf = super.toBuffer(opts, length);
    } else {
      hex = this.toString(HEX_BASE, 2);
      buf = Buffer.from(hex, 'hex');
    }
    return buf;
  }

  public toSMBigEndian = function() {
    let buf;
    if (this.cmp(BitcoreBN.Zero) === -1) {
      buf = this.neg().toBuffer();
      if (buf[0] & NEGATIVE_128) {
        buf = Buffer.concat([Buffer.from([NEGATIVE_128]), buf]);
      } else {
        buf[0] = buf[0] | NEGATIVE_128;
      }
    } else {
      buf = this.toBuffer();
      if (buf[0] & NEGATIVE_128) {
        buf = Buffer.concat([Buffer.from([0x00]), buf]);
      }
    }

    // TODO: Changed & to &&, should be the same
    if (buf.length === 1 && buf[0] === 0) {
      buf = Buffer.from([]);
    }
    return buf;
  };

  public toSM(opts: IBufferEncodingOptions) {
    const endian = opts ? opts.endian : 'big';
    let buf = this.toSMBigEndian();

    if (endian === 'little') {
      buf = reversebuf(buf);
    }
    return buf;
  }

  /**
   * Create a BN from a "ScriptNum":
   * This is analogous to the constructor for CScriptNum in bitcoind. Many ops in
   * bitcoind's script interpreter use CScriptNum, which is not really a proper
   * bignum. Instead, an error is thrown if trying to input a number bigger than
   * 4 bytes. We copy that behavior here. A third argument, `size`, is provided to
   * extend the hard limit of 4 bytes, as some usages require more than 4 bytes.
   */
  public static fromScriptNumBuffer(buf, fRequireMinimal = false, size = 4) {
    const DEFAULT_SIZE = 4;
    const nMaxNumSize = size || DEFAULT_SIZE;
    $.checkArgument(
      buf.length <= nMaxNumSize,
      new Error('script number overflow')
    );
    if (fRequireMinimal && buf.length > 0) {
      // Check that the number is encoded with the minimum possible
      // number of bytes.
      //
      // If the most-significant-byte - excluding the sign bit - is zero
      // then we're not minimal. Note how this test also rejects the
      // negative-zero encoding, 0x80.
      if ((buf[buf.length - 1] & POSITIVE_127) === 0) {
        // One exception: if there's more than one byte and the most
        // significant bit of the second-most-significant-byte is set
        // it would conflict with the sign bit. An example of this case
        // is +-255, which encode to 0xff00 and 0xff80 respectively.
        // (big-endian).
        const TWO = 2;
        const secondToLastIndex = buf.length - TWO;
        if (buf.length <= 1 || (buf[secondToLastIndex] & NEGATIVE_128) === 0) {
          throw new Error('non-minimally encoded script number');
        }
      }
    }
    return BitcoreBN.fromSM(buf, {
      endian: 'little'
    });
  }

  /**
   * The corollary to the above, with the notable exception that we do not throw
   * an error if the output is larger than four bytes. (Which can happen if
   * performing a numerical operation that results in an overflow to more than 4
   * bytes).
   */
  public toScriptNumBuffer() {
    return this.toSM({
      endian: 'little'
    });
  }

  public gt(b: BitcoreBN) {
    return this.cmp(b) > 0;
  }

  public gte(b: BitcoreBN) {
    return this.cmp(b) >= 0;
  }

  public lt(b: BitcoreBN) {
    return this.cmp(b) < 0;
  }

  public static trim(buf: Buffer, natlen: number) {
    return buf.slice(natlen - buf.length, buf.length);
  }

  public static pad(buf: Buffer, natlen: number, size: number) {
    const rbuf = Buffer.alloc(size);
    for (let i = 0; i < buf.length; i++) {
      rbuf[rbuf.length - 1 - i] = buf[buf.length - 1 - i];
    }
    for (let i = 0; i < size - natlen; i++) {
      rbuf[i] = 0;
    }
    return rbuf;
  }
}

// TODO: Why we use this rather than buf.reverse() ?
function reversebuf(buf: Buffer) {
  const buf2 = Buffer.alloc(buf.length);
  for (let i = 0; i < buf.length; i++) {
    buf2[i] = buf[buf.length - 1 - i];
  }
  return buf2;
}
