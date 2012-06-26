/*************************************************************************************************
 * Java binding of Kyoto Cabinet.
 *                                                               Copyright (C) 2009-2011 FAL Labs
 * This file is part of Kyoto Cabinet.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


package kyotocabinet;

import java.util.*;
import java.io.*;
import java.net.*;


/**
 * Library loader.
 */
class Loader {
  //----------------------------------------------------------------
  // package static methods
  //----------------------------------------------------------------
  /**
   * Load the native library.
   */
  static synchronized void load() {
    if(loaded) return;
    System.loadLibrary("jkyotocabinet");
    loaded = true;
  }
  //----------------------------------------------------------------
  // static private fields
  //----------------------------------------------------------------
  /** The flag whether loaded. */
  static private boolean loaded = false;
  //----------------------------------------------------------------
  // private methods
  //----------------------------------------------------------------
  /**
   * Dummy constructor.
   */
  private Loader() {}
}



/* END OF FILE */
