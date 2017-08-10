/*
 * Copyright © 2017 CMCC and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package com.coreos.jetcd.exception;

public class WatchLostException extends EtcdException {

  WatchLostException(String message, Throwable cause) {
    super(message, cause);
  }

}
