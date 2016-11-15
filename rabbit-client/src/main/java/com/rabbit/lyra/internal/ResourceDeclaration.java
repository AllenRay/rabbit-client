package com.rabbit.lyra.internal;


import com.rabbit.lyra.internal.util.Reflection;

import java.lang.reflect.Method;

/**
 * Encapsulates a named resource declaration.
 * 
 * @author Jonathan Halterman
 */
class ResourceDeclaration {
  final Method method;
  final Object[] args;

  ResourceDeclaration(Method method, Object[] args) {
    this.method = method;
    this.args = args;
  }

  <T> T invoke(Object subject) throws Exception {
    return Reflection.invoke(subject, method, args);
  }
}
