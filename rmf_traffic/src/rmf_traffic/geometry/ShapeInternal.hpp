/*
 * Copyright (C) 2019 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

#ifndef SRC__RMF_TRAFFIC__GEOMETRY__SHAPEINTERNAL_HPP
#define SRC__RMF_TRAFFIC__GEOMETRY__SHAPEINTERNAL_HPP

#include <rmf_traffic/geometry/Shape.hpp>
#include <rmf_traffic/geometry/ConvexShape.hpp>

#ifdef RMF_TRAFFIC__USING_FCL_0_6
#include <fcl/narrowphase/collision_object.h>
#else
#include <fcl/collision_object.h>
#endif

#include <vector>
#include <rmf_utils/optional.hpp>
#include <Eigen/Dense>

namespace rmf_traffic {
namespace geometry {

#ifdef RMF_TRAFFIC__USING_FCL_0_6
using CollisionGeometryPtr = std::shared_ptr<fcl::CollisionGeometryd>;
#else
using CollisionGeometryPtr = std::shared_ptr<fcl::CollisionGeometry>;
#endif
using CollisionGeometries = std::vector<CollisionGeometryPtr>;

//==============================================================================
/// \brief Implementations of this class must be created by the child classes of
/// Shape, and then passed to the constructor of Shape.
class Shape::Internal
{
public:

  virtual CollisionGeometries make_fcl() const = 0;

  virtual ~Internal() = default;

};

//==============================================================================
class FinalShape::Implementation
{
public:

  rmf_utils::impl_ptr<const Shape> _shape;

  CollisionGeometries _collisions;

  double _characteristic_length;

  rmf_utils::optional<Eigen::Vector2d> _offset;

  static const CollisionGeometries& get_collisions(const FinalShape& shape)
  {
    return shape._pimpl->_collisions;
  }

  static FinalShape make_final_shape(
    rmf_utils::impl_ptr<const Shape> shape,
    CollisionGeometries collisions,
    double characteristic_length)
  {
    FinalShape result;
    result._pimpl = rmf_utils::make_impl<Implementation>(
      Implementation{std::move(shape),
        std::move(collisions),
        std::move(characteristic_length),
        std::nullopt});
    return result;
  }

  static FinalShape make_final_shape_with_offset(
    rmf_utils::impl_ptr<const Shape> shape,
    CollisionGeometries collisions,
    double characteristic_length,
    Eigen::Vector2d offset)
  {
    FinalShape result;
    result._pimpl = rmf_utils::make_impl<Implementation>(
      Implementation{std::move(shape),
        std::move(collisions),
        std::move(characteristic_length),
        offset});
    return result;
  }
};

//==============================================================================
class FinalConvexShape::Implementation
{
public:

  static CollisionGeometryPtr get_collision(const FinalConvexShape& shape)
  {
    return shape._pimpl->_collisions.front();
  }

  static FinalConvexShape make_final_shape(
    rmf_utils::impl_ptr<const Shape> shape,
    CollisionGeometries collisions,
    double characteristic_length)
  {
    FinalConvexShape result;
    result._pimpl = rmf_utils::make_impl<FinalShape::Implementation>(
      FinalShape::Implementation{std::move(shape),
        std::move(collisions),
        characteristic_length,
        std::nullopt});
    return result;
  }

  static FinalConvexShape make_final_shape_with_offset(
    rmf_utils::impl_ptr<const Shape> shape,
    CollisionGeometries collisions,
    double characteristic_length,
    Eigen::Vector2d offset)
  {
    FinalConvexShape result;
    result._pimpl = rmf_utils::make_impl<FinalShape::Implementation>(
      FinalShape::Implementation{std::move(shape),
        std::move(collisions),
        characteristic_length,
        offset});
    return result;
  }
};

} // namespace geometry
} // namespace rmf_traffic

#endif // SRC__RMF_TRAFFIC__GEOMETRY__SHAPEINTERNAL_HPP
