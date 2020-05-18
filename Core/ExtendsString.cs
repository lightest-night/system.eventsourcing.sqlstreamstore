using System;
using LightestNight.System.Utilities.Extensions;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsString
    {
        public static string GetCategoryStreamId(this string target)
            => $"{(target.ThrowIfNull().StartsWith(Constants.CategoryPrefix, StringComparison.InvariantCultureIgnoreCase) ? string.Empty : Constants.CategoryPrefix)}{target}";
    }
}