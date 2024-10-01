package tabletypesgen.util;

import java.util.Set;
import java.util.regex.Pattern;

import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Locale.ENGLISH;


public final class StringFuns
{
   private static final String SPACES = "                                                                              ";

   public static String makeNameNotInSet
      (
         String baseName,
         Set<String> existingNames
      )
   {
      return makeNameNotInSet(baseName, existingNames, "");
   }

   public static int countOccurrences
      (
         String s,
         char c
      )
   {
      int count = 0;
      for (int i = 0; i < s.length(); ++i)
         if ( s.charAt(i) == c ) ++count;
      return count;
   }

   public static String makeNameNotInSet
      (
         String baseName,
         Set<String> existingNames,
         String suffixSep
      )
   {
      if ( !existingNames.contains(baseName) )
         return baseName;
      else
      {
         int i = 1;
         while ( existingNames.contains(baseName + suffixSep + i) ) ++i;
         return baseName + suffixSep + i;
      }
   }

   public static String lowercaseInitials
      (
         String name,
         String sep
      )
   {
      StringBuilder sb = new StringBuilder();

      for ( String word: name.split(sep) )
      {
         if ( word.length() > 0 )
            sb.append(Character.toLowerCase(word.charAt(0)));
      }

      return sb.toString();
   }

   /// Return camel-case form of the given string, where words separated by
   /// '_', ' ', or '-' characters are combined and the initial letter of the
   /// returned name is capitalized.
   public static String upperCamelCase(String name)
   {
      StringBuilder res = new StringBuilder();
      for (String word : name.split("[_ -]"))
      {
         res.append(Character.toUpperCase(word.charAt(0)));
         res.append(word.substring(1).toLowerCase());
      }
      return res.toString();
   }

   public static String lowerCamelCase(String name)
   {
      StringBuilder res = new StringBuilder();
      for (String word : name.split("[_ -]"))
      {
         if ( res.length() == 0 )
            res.append(word.toLowerCase());
         else
         {
            res.append(Character.toUpperCase(word.charAt(0)));
            res.append(word.substring(1).toLowerCase());
         }
      }
      return res.toString();
   }

   public static String capitalize(String name)
   {
      if ( name.isEmpty() ) return name;
      else return name.substring(0, 1).toUpperCase(ENGLISH) + name.substring(1);
   }

   public static String indentLines
      (
         String linesStr,
         int spacesCount
      )
   {
      return indentLines(linesStr, spacesCount, true);
   }

   public static String indentLines
      (
         String linesStr,
         int spacesCount,
         boolean indentFirstLine
      )
   {
      StringBuilder sb = new StringBuilder();

      CharSequence indention = SPACES.subSequence(0, Math.min(spacesCount, SPACES.length()));

      boolean pastFirst = false;
      for ( String line : linesStr.split("\n") )
      {
         if ( pastFirst ) sb.append('\n');
         if ( pastFirst || indentFirstLine ) sb.append(indention);
         sb.append(line);
         if ( !pastFirst ) pastFirst = true;
      }

      return sb.toString();
   }

   public static String unDoubleQuote(String s)
   {
      if ( s.startsWith("\"") && s.endsWith("\"") )
         return s.substring(1, s.length()-1);
      return s;
   }

   public static boolean matches
      (
         @Nullable Pattern pat,
         String s
      )
   {
      return pat != null && pat.matcher(s).matches();
   }

   public static String maybeQualify
      (
         @Nullable String qualifier,
         String objectName
      )
   {
      return (qualifier != null ? qualifier + "." : "") + objectName;
   }

   public static String replaceStringsInWith
      (
         String s,
         String s1,
         String v1,
         String s2,
         String v2
      )
   {
      return ( s2.length() >= s1.length() ) ?
         s.replace(s2, v2).replace(s1, v1)
         : s.replace(s1, v1).replace(s2, v2);
   }

   private StringFuns() {}
}
