/*
 * Copyright (C) 2021 Open Source Robotics Foundation
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

#ifndef RMF_TRAFFIC__SCHEDULE__NEGOTIATION_ROOM_HPP
#define RMF_TRAFFIC__SCHEDULE__NEGOTIATION_ROOM_HPP

#include <rmf_traffic/schedule/Negotiator.hpp>
#include <rmf_traffic/schedule/Participant.hpp>
#include <rmf_traffic/schedule/Snapshot.hpp>

#include <rmf_utils/impl_ptr.hpp>

#include <list>

namespace rmf_traffic {
namespace schedule {

//==============================================================================
/// A interface for negotiating solutions to schedule conflicts
class NegotiationRoom
{
public:

  /// The Worker class can be used to make the Negotiation asynchronous.
  class Worker
  {
  public:

    /// Tell the worker to add a callback to its schedule. It is imperative that
    /// this function is thread-safe.
    virtual void schedule(std::function<void()> job) = 0;

    virtual ~Worker() = default;
  };


  struct Repeat
  {
    // Repeat conflict information related to this version
    uint64_t conflict_version;

    // Repeat conflict information related to this table. If this is empty, then
    // only the initial NegotiationNotice will be repeated.
    std::vector<uint64_t> table;
  };

  struct Notice
  {
    // The version number assigned to this conflict
    uint64_t conflict_version;

    // The IDs of the participants that are in conflict.
    std::vector<uint64_t> participants;
  };

  struct Refusal
  {
    // The ID of the conflict negotiation that is being refused
    uint64_t conflict_version;
  };

  struct Key
  {
    // The participant ID of the negotiation table
    uint64_t participant;

    // The version of the negotiation table that we care about
    uint64_t version;
  };

  struct Proposal
  {
    // The conflict ID that this proposal is targeted at
    uint64_t conflict_version;

    // The version number for this proposal within the negotiation
    uint64_t proposal_version;

    // The participant ID that this proposal is coming from
    uint64_t for_participant;

    // The participant IDs that this proposal is trying to accommodate. As each
    // participant proposes their ideal itinerary, the other participants in the
    // conflict will propose itineraries which accommodate it.
    //
    // The order of IDs in this dynamic array have important semantic meaning about
    // which itineraries are being accommodated. For example:
    //
    // [] (empty):  This proposal does not accommodate any other participants. This
    //              is the best possible itinerary for this participant.
    //
    // [3]:         This proposal is the best itinerary that can accommodate the
    //              ideal itinerary of participant 3.
    //
    // [3, 7]:      This proposal is the best itinerary for this participant that can
    //              accommodate both the ideal itinerary of participant 3 and the
    //              best itinerary of participant 7 that accommodates the ideal
    //              itinerary of participant 3.
    //
    // [3, 7, ...]: This proposal is the best itinerary that can accommodate the
    //              ideal itinerary of participant 3 and the best itineraries that
    //              accommodate the best itineraries of the participants that precede
    //              them in the list, recursively.
    std::vector<Key> to_accommodate;

    // The itinerary that is being proposed for this participant
    std::vector<Route> itinerary;
  };

  struct Rejection
  {
    // The conflict ID that this rejection is targeted at
    uint64_t conflict_version;

    // Reject this negotiation table
    std::vector<Key> table;

    // The rejection is by this participant
    uint64_t rejected_by;

    std::vector<Itinerary> alternatives;
  };

  struct Forfeit
  {
    // The conflict ID that this forfeit is targeted at
    uint64_t conflict_version;

    // Forfeit this negotiation table
    std::vector<Key> table;
  };

  struct Conclusion
  {
    // The version number assigned to this conflict
    uint64_t conflict_version;

    // True if the conflict was resolved. False if the negotiation was abandoned.
    bool resolved;

    // The ID sequence for the negotiation table that was selected
    std::vector<Key> table;
  };

  struct ParticipantAck
  {

    // The participant that is acknowledging
    uint64_t participant;

    // The itinerary version that will provide the update to
    // conform to the negotiation result
    uint64_t itinerary_version;

    // Whether this participant will be updating
    bool updating = false;
  };

  struct Ack
  {
    // The version number of the conflict whose conclusion is being acknowledged
    uint64_t conflict_version;

    // The participants who are acknowledging the conclusion of the conflict negotiation
    std::vector<ParticipantAck> acknowledgments;
  };

  struct PublisherCallbacks
  {
    std::function<void(const Refusal& refusal)> refusal;

    std::function<void(const Ack& ack)> ack;

    std::function<void(const Proposal& proposal)> proposal;

    std::function<void(const Rejection& rejection)> rejection;

    std::function<void(const Forfeit& forfeit)> forfeit;
  };

  struct SubscriptionCallbacks
  {
    std::function<void(const Repeat& msg)> repeat;

    std::function<void(const Notice& notice)> notice;

    std::function<void(const Proposal& proposal)> proposal;

    std::function<void(const Rejection& rejection)> rejection;

    std::function<void(const Forfeit& forfeit)> forfeit;

    std::function<void(const Conclusion& conclusion)> conclusion;
  };

  /// Constructor
  ///
  /// \param[in] worker
  ///   If a worker is provided, the Negotiation will be performed
  ///   asynchronously. If it is not provided, then the Negotiators must be
  ///   single-threaded, and their respond() functions must block until
  ///   finished.
  NegotiationRoom(
    std::shared_ptr<const Snappable> viewer,
    PublisherCallbacks publisher_callbacks,
    std::shared_ptr<SubscriptionCallbacks> subscription_callbacks,
    std::shared_ptr<Worker> worker = nullptr);

  /// Set the timeout duration for negotiators. If a negotiator does not respond
  /// within this time limit, then the negotiation will automatically be
  /// forfeited. This is important to prevent negotiations from getting hung
  /// forever.
  NegotiationRoom& timeout_duration(rmf_traffic::Duration duration);

  /// Get the current timeout duration setting.
  rmf_traffic::Duration timeout_duration() const;

  using TableViewPtr = Negotiation::Table::ViewerPtr;
  using StatusUpdateCallback =
    std::function<void (uint64_t conflict_version, TableViewPtr table_view)>;

  /// Register a callback with this Negotiation manager that triggers
  /// on negotiation status updates.
  ///
  /// \param[in] cb
  ///   The callback function to be called upon status updates.
  void on_status_update(StatusUpdateCallback cb);

  using StatusConclusionCallback =
    std::function<void (uint64_t conflict_version, bool success)>;

  /// Register a callback with this Negotiation manager that triggers
  /// on negotiation status conclusions.
  ///
  /// \param[in] cb
  ///   The callback function to be called upon status conclusions.
  void on_conclusion(StatusConclusionCallback cb);

  /// Get a Negotiation::TableView that provides a view into what participants are
  /// proposing.
  ///
  /// This function does not care about table versioning.
  /// \param[in] conflict_version
  ///   The conflict version of the negotiation
  /// \param[in] sequence
  ///   The sequence of participant ids. Follows convention of other sequences
  ///   (ie. The last ParticipantId is the owner of the table)
  ///
  /// \return A TableView into what participants are proposing.
  TableViewPtr table_view(
    uint64_t conflict_version,
    const std::vector<ParticipantId>& sequence) const;

  /// Set the number of negotiations to retain.
  ///
  /// \param[in] count
  ///   The number of negotiations to retain
  void set_retained_history_count(uint count);

  /// Register a negotiator with this Negotiation manager.
  ///
  /// \param[in] for_participant
  ///   The ID of the participant that this negotiator will work for
  ///
  /// \param[in] negotiator
  ///   The negotiator interface to use for this participant
  ///
  /// \return a handle that should be kept by the caller. When this handle
  /// expires, this negotiator will be automatically unregistered.
  std::shared_ptr<void> register_negotiator(
    ParticipantId for_participant,
    std::unique_ptr<Negotiator> negotiator);

  class Implementation;

  class Conversion;

private:
  rmf_utils::unique_impl_ptr<Implementation> _pimpl;
};

class NegotiationRoomInternal
{
public:
  NegotiationRoomInternal(Negotiation negotiation_);

  Negotiation negotiation;
  std::list<NegotiationRoom::Proposal> cached_proposals;
  std::list<NegotiationRoom::Rejection> cached_rejections;
  std::list<NegotiationRoom::Forfeit> cached_forfeits;

  using NegotiatorPtr = std::unique_ptr<Negotiator>;
  using NegotiatorMap = std::unordered_map<ParticipantId, NegotiatorPtr>;

  std::vector<Negotiation::TablePtr> check_cache(
    const NegotiatorMap& negotiator_map);
};

//==============================================================================
void print_negotiation_status(
  Version conflict_version,
  const Negotiation& negotiation);

} // namespace schedule
} // namespace rmf_traffic

#endif // RMF_TRAFFIC__SCHEDULE__NEGOTIATION_ROOM_HPP
